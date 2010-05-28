open Lwt_preemptive
open Lwt

type conn_pool = cpool Lazy.t
and cpool =
    {
      mutable servers : (string * int) array;
      pool : (Cassandra.connection * Cassandra.keyspace) Lwt_pool.t;
    }
    

let check_conn (conn, _) f =
  (* TODO: dummy request to make sure the conn is OK? *)
  try
    f (Cassandra.valid_connection conn)
  with _ -> f false

let make_pool servers ?credentials ?level ?rewrite_keys ~keyspace max_conns =
  let rec cp = 
    lazy {
      servers = Array.of_list servers;
      pool = Lwt_pool.create max_conns ~check:check_conn create;
    }
  and create () =
    let cp = Lazy.force cp in
      if cp.servers = [||] then failwith "No servers available"
      else
        detach
          (fun (host, port) -> 
             let conn = Cassandra.connect ~host port in
             let ks = Cassandra.get_keyspace conn ?level ?rewrite_keys keyspace in
               begin match credentials with
                   Some [] | None -> ()
                 | Some l -> Cassandra.login ks l
               end;
               (conn, ks))
          cp.servers.(Random.int (Array.length cp.servers))
  in cp

let rec with_ks t f =
  try
    Lwt_pool.use (Lazy.force t).pool (detach (fun (_, ks) -> f ks))
  with e -> fail e (* FIXME: retry with other connection *)

let get t ?level ~cf ~key ?sc col =
  with_ks t (fun ks -> Cassandra.get ks ?level ~cf ~key ?sc col)

let get_value t ?level ~cf ~key ?sc col =
  lwt col = get t ?level ~cf ~key ?sc col in
    return col.Cassandra.c_value

let get' t ?level ~cf ~key col =
  with_ks t (fun ks -> Cassandra.get' ks ?level ~cf ~key col)

let get_supercolumn = get'
                        
let get_slice t ?level ~cf ~key ?sc pred =
  with_ks t (fun ks -> Cassandra.get_slice ks ?level ~cf ~key ?sc pred)

let get_superslice t ?level ~cf ~key pred =
  with_ks t (fun ks -> Cassandra.get_superslice ks ?level ~cf ~key pred)

let multiget_slice t ?level ~cf keys ?sc pred =
  with_ks t (fun ks -> Cassandra.multiget_slice ks ?level ~cf keys ?sc pred)

let multiget_superslice t ?level ~cf keys pred =
  with_ks t (fun ks -> Cassandra.multiget_superslice ks ?level ~cf keys pred)

let count t ?level ~cf ~key ?sc () =
  with_ks t (fun ks -> Cassandra.count ks ?level ~cf ~key ?sc ())

let get_range_slices t ?level ~cf ?sc pred range =
  with_ks t (fun ks -> Cassandra.get_range_slices ks ?level ~cf ?sc pred range)

let get_range_superslices t ?level ~cf pred range =
  with_ks t (fun ks -> Cassandra.get_range_superslices ks ?level ~cf pred range)

let insert t ?level ~cf ~key ?sc ~name ?timestamp value =
  with_ks t (fun ks -> Cassandra.insert ks ?level ~cf ~key ?sc ~name ?timestamp value)

let insert_supercolumn t ?level ~cf ~key ~name ?timestamp l =
  with_ks t
    (fun ks -> Cassandra.insert_supercolumn ks ?level ~cf ~key ~name ?timestamp l)

let insert_column t ?level ~cf ~key ?sc ?timestamp column =
  with_ks t
    (fun ks -> Cassandra.insert_column ks ?level ~cf ~key ?sc ?timestamp column)

let remove_key t ?level ~cf ?timestamp key =
  with_ks t (fun ks -> Cassandra.remove_key ks ?level ~cf ?timestamp key)

let remove_column t ?level ~cf ~key ?sc ?timestamp name =
  with_ks t
    (fun ks -> Cassandra.remove_column ks ?level ~cf ~key ?sc ?timestamp name)

let remove_supercolumn t ?level ~cf ~key ?timestamp name =
  with_ks t
    (fun ks -> Cassandra.remove_supercolumn ks ?level ~cf ~key ?timestamp name)

let batch_mutate t ?level l =
  with_ks t (fun ks -> Cassandra.batch_mutate ks ?level l)

module Typed =
struct
  include Cassandra.Typed

  let get t ?level col ~key =
    with_ks t (fun ks -> get ks ?level col ~key)

  let set t ?level col ~key ?timestamp v =
    with_ks t (fun ks -> set ks ?level col ~key ?timestamp v)

  let get' t ?level ~sc col ~key =
    with_ks t (fun ks -> get' ks ?level ~sc col ~key)

  let set' t ?level ~sc col ~key ?timestamp v =
    with_ks t (fun ks -> set' ks ?level ~sc col ~key ?timestamp v)
end
