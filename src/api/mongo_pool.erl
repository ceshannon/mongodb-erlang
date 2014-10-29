-module(mongo_pool).
-author('liuhao@worktile.com').

-export([start_link/6,
	 init/1,
	 child_spec/6,
	 insert/3,
	 update/4,
	 update/5,
	 update/6,
	 delete/3,
	 delete_one/3]).

-export([
	 find_one/3,
	 find_one/4,
	 find_one/5,
	 find/3,
	 find/4,
	 find/5,
	 find/6
	]).

-include("mongo_protocol.hrl").

-define(CONNECT_TIMEOUT, 1000).

-type cursor() :: pid().

child_spec(PoolName, PoolSize, Server, Port, Db, MaxOverflow) ->
    SupervisorName = ?MODULE,
    {SupervisorName,
     {?MODULE, start_link, [PoolName, PoolSize, Server, Port, Db, MaxOverflow]},
     transient,
     infinity,
     supervisor,
     [?MODULE]}.


start_link(PoolName, PoolSize, Server, Port, Db, MaxOverflow) -> 
    supervisor:start_link({local, ?MODULE}, ?MODULE, [PoolName, PoolSize, Server, Port, Db, MaxOverflow]).

init([PoolName, PoolSize, Server, Port, Db, MaxOverflow]) ->
    PoolArgs = [
		{name, {local, PoolName}},
		{worker_module, mc_worker},
		{size, PoolSize},
		{max_overflow, MaxOverflow}
	       ],
    WorkerArgs = [{Server, Port, {conn_state, unsafe, master, Db}}, [{timeout, ?CONNECT_TIMEOUT}]],
    {ok, {{one_for_one, 10, 10}, [poolboy:child_spec(PoolName, PoolArgs, WorkerArgs)]}}.

insert(Pool, Coll, Doc) when is_tuple(Doc) ->
	hd(insert(Pool, Coll, [Doc]));
insert(Pool, Coll, Docs) ->
	Docs1 = [assign_id(Doc) || Doc <- Docs],
	request(Pool, #insert{collection = Coll, documents = Docs1}),
	Docs1.

-spec update(pid(), collection(), selector(), bson:document()) -> ok.
update(Pool, Coll, Selector, Doc) ->
	update(Pool, Coll, Selector, Doc, false, false).

%% @doc Replace the document matching criteria entirely with the new Document.
-spec update(pid(), collection(), selector(), bson:document(), boolean()) -> ok.
update(Pool, Coll, Selector, Doc, Upsert) ->
	update(Pool, Coll, Selector, Doc, Upsert, false).

%% @doc Replace the document matching criteria entirely with the new Document.
-spec update(pid(), collection(), selector(), bson:document(), boolean(), boolean()) -> ok.
update(Pool, Coll, Selector, Doc, Upsert, MultiUpdate) ->
	request(Pool, #update{collection = Coll, selector = Selector, updater = Doc, upsert = Upsert, multiupdate = MultiUpdate}).

%% @doc Delete selected documents
-spec delete(pid(), collection(), selector()) -> ok.
delete(Pool, Coll, Selector) ->
	request(Pool, #delete{collection = Coll, singleremove = false, selector = Selector}).

%% @doc Delete first selected document.
-spec delete_one(pid(), collection(), selector()) -> ok.
delete_one(Pool, Coll, Selector) ->
	request(Pool, #delete{collection = Coll, singleremove = true, selector = Selector}).

%% @doc Return first selected document, if any
-spec find_one(pid(), collection(), selector()) -> {} | {bson:document()}.
find_one(Pool, Coll, Selector) ->
	find_one(Pool, Coll, Selector, []).

%% @doc Return projection of first selected document, if any. Empty projection [] means full projection.
-spec find_one(pid(), collection(), selector(), projector()) -> {} | {bson:document()}.
find_one(Pool, Coll, Selector, Projector) ->
	find_one(Pool, Coll, Selector, Projector, 0).

%% @doc Return projection of Nth selected document, if any. Empty projection [] means full projection.
-spec find_one(pid(), collection(), selector(), projector(), skip()) -> {} | {bson:document()}.
find_one(Pool, Coll, Selector, Projector, Skip) ->
	read_one(Pool, #'query'{
		collection = Coll,
		selector = Selector,
		projector = Projector,
		skip = Skip
	}).

%% @doc Return selected documents.
-spec find(pid(), collection(), selector()) -> cursor().
find(Pool, Coll, Selector) ->
	find(Pool, Coll, Selector, []).

%% @doc Return projection of selected documents.
%%      Empty projection [] means full projection.
-spec find(pid(), collection(), selector(), projector()) -> cursor().
find(Pool, Coll, Selector, Projector) ->
	find(Pool, Coll, Selector, Projector, 0).

%% @doc Return projection of selected documents starting from Nth document.
%%      Empty projection means full projection.
-spec find(pid(), collection(), selector(), projector(), skip()) -> cursor().
find(Pool, Coll, Selector, Projector, Skip) ->
	find(Pool, Coll, Selector, Projector, Skip, 0).

%% @doc Return projection of selected documents starting from Nth document in batches of batchsize.
%%      0 batchsize means default batch size.
%%      Negative batch size means one batch only.
%%      Empty projection means full projection.
-spec find(pid(), collection(), selector(), projector(), skip(), batchsize()) -> cursor(). % Action
find(Pool, Coll, Selector, Projector, Skip, BatchSize) ->
	read(Pool, #'query'{
		collection = Coll,
		selector = Selector,
		projector = Projector,
		skip = Skip,
		batchsize = BatchSize
	}).


request(Pool, Request) ->  %request to worker
    Timeout = case application:get_env(mc_worker_call_timeout) of
		  {ok, Time} -> Time;
		  undefined -> infinity
	      end,
    poolboy:transaction(Pool, fun(Worker) ->
				      reply(gen_server:call(Worker, Request, Timeout)) end).

read(Pool, Request = #'query'{collection = Collection, batchsize = BatchSize} ) ->
    Timeout = case application:get_env(mc_worker_call_timeout) of
		  {ok, Time} -> Time;
		  undefined -> infinity
	      end,
    poolboy:transaction(Pool, fun(Worker) ->
				      {Cursor, Batch} = reply(gen_server:call(Worker, Request, Timeout)),
				      mc_cursor:create(Worker, Collection, Cursor, BatchSize, Batch)
			      end).

read_one(Pool, Request) ->
	{0, Docs} = request(Pool, Request#'query'{batchsize = -1}),
	case Docs of
		[] -> {};
		[Doc | _] -> {Doc}
	end.

assign_id(Doc) ->
	case bson:lookup('_id', Doc) of
		{_Value} -> Doc;
		{} -> bson:update('_id', mongo_id_server:object_id(), Doc)
	end.

reply(ok) -> ok;
reply(#reply{cursornotfound = false, queryerror = false} = Reply) ->
	{Reply#reply.cursorid, Reply#reply.documents};
reply(#reply{cursornotfound = false, queryerror = true} = Reply) ->
	[Doc | _] = Reply#reply.documents,
	process_error(bson:at(code, Doc), Doc);
reply(#reply{cursornotfound = true, queryerror = false} = Reply) ->
	erlang:error({bad_cursor, Reply#reply.cursorid}).

process_reply(Doc, Command) ->
	case bson:lookup(ok, Doc) of
		{N} when N == 1 -> {true, bson:exclude([ok], Doc)};   %command succeed
		{N} when N == 0 -> {false, bson:exclude([ok], Doc)};  %command failed
		_Res -> erlang:error({bad_command, Doc}, [Command]) %unknown result
	end.

process_error(13435, _) ->
	erlang:error(not_master);
process_error(10057, _) ->
	erlang:error(unauthorized);
process_error(_, Doc) ->
	erlang:error({bad_query, Doc}).

pw_key(Nonce, Username, Password) ->
	bson:utf8(binary_to_hexstr(crypto:hash(md5, [Nonce, Username, pw_hash(Username, Password)]))).

%% @private
pw_hash(Username, Password) ->
	bson:utf8(binary_to_hexstr(crypto:hash(md5, [Username, <<":mongo:">>, Password]))).

%% @private
binary_to_hexstr(Bin) ->
	lists:flatten([io_lib:format("~2.16.0b", [X]) || X <- binary_to_list(Bin)]).

value_to_binary(Value) when is_integer(Value) ->
	bson:utf8(integer_to_list(Value));
value_to_binary(Value) when is_atom(Value) ->
	atom_to_binary(Value, utf8);
value_to_binary(Value) when is_binary(Value) ->
	Value;
value_to_binary(_Value) ->
	<<>>.
