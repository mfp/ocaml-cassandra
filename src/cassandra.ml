(* Copyright (c) 2009 Mauricio Fernández <mfp@acm.org> *)

IFDEF EXTLIB THEN
  open ExtList
  open ExtHashtbl
  open ExtString
ELSE
  module Option = BatOption
  module Hashtbl =
  struct
    include BatHashtbl
    let map f h = map (fun k v -> f v) h
  end
  module List = struct include List include BatList end
  module String = struct include String include BatString end
ENDIF

open Cassandra_thrift
open Cassandra_types

let exn_printer =
  let describe s o = Some (Printf.sprintf "%s(%S)" s (Option.default "?" o#get_why)) in
  function
    | InvalidRequestException o -> describe "InvalidRequestException" o
    | AuthenticationException o -> describe "AuthenticationException" o
    | AuthorizationException o  -> describe "AuthorizationException" o
    | _ -> None

type timestamp = Int64.t
type column = { c_name : string; c_value : string; c_timestamp : timestamp; }
type supercolumn = { sc_name : string; sc_columns : column list }

type level = [ `ZERO | `ONE | `QUORUM | `DCQUORUM | `DCQUORUMSYNC | `ALL | `ANY ]

type access_level = [ `NONE | `READONLY | `READWRITE | `FULL ]

type slice_predicate =
    [ `Columns of string list | `Column_range of string * string * bool * int ]

type key_range =
    [ `Key of string * string * int | `Token of string * string * int]

type key_slice = string * column list
type key_superslice = string * supercolumn list

type mutation =
    [
      `Delete of timestamp option *
        [ `Key | `Super_column of string | `Columns of slice_predicate
        | `Sub_columns of string * slice_predicate ]
    | `Insert of column
    | `Insert_super of supercolumn
    ]

type connection = {
  proto : Thrift.Protocol.t;
  client : Cassandra.client;
}

module M = Map.Make(String)

type key_rewriter = { map : string -> string; unmap : string -> string }

type keyspace = {
  ks_client : Cassandra.client;
  ks_level : level;
  ks_rewrite : key_rewriter M.t;
}

let key_rewriter ~map ~unmap = { map = map; unmap = unmap }

let digest_rewriter =
  let map s =
    let d = Digest.to_hex (Digest.string s) in
      String.slice ~last:5 d ^ "-" ^ s in
  let unmap s = String.slice ~first:6 s in
    { map = map; unmap = unmap }

let make_timestamp () = Int64.of_float (1e6 *. Unix.gettimeofday ())

let connect ?(framed=true) ~host port =
  let tx = new TSocket.t host port in
  let tx = if framed then new TFramedTransport.t tx else tx in
  let proto = new TBinaryProtocol.t tx in
  let client = new Cassandra.client proto proto in
    tx#opn;
    { proto = proto; client = client; }

let disconnect t =
  let tx = t.proto#getTransport in
    if tx#isOpen then tx#close

let reconnect t =
  let tx = t.proto#getTransport in
    if not tx#isOpen then tx#opn

let valid_connection t =
  let tx = t.proto#getTransport in
    tx#isOpen

let set_keyspace t ?(level = `ONE) ?(rewrite_keys = []) name =
  let rewrite_map =
    List.fold_left (fun m (cf, rw) -> M.add cf rw m) M.empty rewrite_keys
  in
    t.client#set_keyspace name;
    { ks_client = t.client; ks_level = level;
      ks_rewrite = rewrite_map;
    }

open AccessLevel

let of_access_level = function
  | NONE -> `NONE
  | READONLY -> `READONLY
  | READWRITE -> `READWRITE
  | FULL -> `FULL

let login ks credentials =
  let auth = new authenticationRequest in
  let h = Hashtbl.create 13 in
    List.iter (fun (k, v) -> Hashtbl.add h k v) credentials;
    auth#set_credentials h;
    of_access_level (ks.ks_client#login auth)

open ConsistencyLevel

let consistency_level = function
  | `ZERO -> ZERO
  | `ONE -> ONE
  | `QUORUM -> QUORUM
  | `DCQUORUM -> DCQUORUM
  | `DCQUORUMSYNC -> DCQUORUMSYNC
  | `ALL -> ALL
  | `ANY -> ANY

let clevel ks =
  Option.map_default consistency_level (consistency_level ks.ks_level)

let mk_clock t = let c = new clock in c#set_timestamp t; c

let column c =
  let r = new column in
    r#set_name c.c_name;
    r#set_value c.c_value;
    r#set_clock (mk_clock c.c_timestamp);
    r

let of_column c =
  {
    c_name = c#grab_name; c_value = c#grab_value;
    c_timestamp = c#grab_clock#grab_timestamp;
  }

let supercolumn c =
  let r = new superColumn in
    r#set_name c.sc_name;
    r#set_columns (List.map column c.sc_columns);
    r

let of_super_column c =
  { sc_name = c#grab_name; sc_columns = List.map of_column c#grab_columns; }

let column_path ~cf ?sc c =
  let r = new columnPath in
    r#set_column_family cf;
    Option.may r#set_super_column sc;
    r#set_column c;
    r

let supercolumn_path ~cf sup =
  let r = new columnPath in
    r#set_column_family cf;
    r#set_super_column sup;
    r

let column_parent ?sc cf =
  let o = new columnParent in
    o#set_column_family cf;
    Option.may o#set_super_column sc;
    o

let slice_predicate p =
  let r = new slicePredicate in
    begin match p with
        `Columns cs -> r#set_column_names cs
      | `Column_range (start, finish, reversed, count) ->
          let range = new sliceRange in
            range#set_start start;
            range#set_finish finish;
            range#set_reversed reversed;
            range#set_count count;
            r#set_slice_range range
    end;
    r

let get_columns = List.filter_map (fun r -> Option.map of_column r#get_column)

let get_supercolumns =
  List.filter_map (fun r -> Option.map of_super_column r#get_super_column)

let map_key ks ~cf key =
  try
    (M.find cf ks.ks_rewrite).map key
  with Not_found -> key

let key_range t cf r =
  let map = function "" -> "" | s -> map_key t cf s in
  let o = new keyRange in
    begin
      match r with
          `Key (start, stop, count) -> o#set_start_key (map start);
                                       o#set_end_key (map stop);
                                       o#set_count count
        | `Token (start, stop, count) -> o#set_start_token (map start);
                                         o#set_end_token (map stop);
                                         o#set_count count
    end;
    o

let unmap_key ks ~cf key' =
  try
    (M.find cf ks.ks_rewrite).unmap key'
  with Not_found -> key'

let of_key_slice t cf r =
  (unmap_key t cf r#grab_key, get_columns r#grab_columns)

let of_key_super_slice t cf r =
  (unmap_key t cf r#grab_key, get_supercolumns r#grab_columns)

let get t ?level ~cf ~key ?sc column =
  let r = t.ks_client#get
            (map_key t ~cf key) (column_path ~cf ?sc column) (clevel t level)
  in of_column r#grab_column

let get_value t ?level ~cf ~key ?sc col =
  (get t ~key ?level ~cf ?sc col).c_value

let get' t ?level ~cf ~key name =
  let r = t.ks_client#get
            (map_key t ~cf key) (supercolumn_path ~cf name) (clevel t level)
  in of_super_column r#grab_super_column

let get_supercolumn = get'

let get_slice t ?level ~cf ~key ?sc pred =
  let cols =
    t.ks_client#get_slice (map_key t ~cf key)
      (column_parent cf ?sc)
      (slice_predicate pred) (clevel t level)
  in get_columns cols

let get_superslice t ?level ~cf ~key pred =
  let cols =
    t.ks_client#get_slice (map_key t ~cf key)
      (column_parent cf) (slice_predicate pred) (clevel t level)
  in get_supercolumns cols

let multiget_slice t ?level ~cf keys ?sc pred =
  let h =
    t.ks_client#multiget_slice (List.map (map_key t ~cf) keys)
      (column_parent cf ?sc)
      (slice_predicate pred) (clevel t level) in
  let to_cols l = List.map (fun r -> of_column r#grab_column) l
  in
    try
      let unmap = (M.find cf t.ks_rewrite).unmap in
      let h' = Hashtbl.create (Hashtbl.length h) in
        Hashtbl.iter (fun k v -> Hashtbl.add h' (unmap k) (to_cols v)) h;
        h'
    with Not_found -> Hashtbl.map to_cols h

let multiget_superslice t ?level ~cf keys pred =
  let h =
    t.ks_client#multiget_slice (List.map (map_key t ~cf) keys)
      (column_parent cf) (slice_predicate pred) (clevel t level) in
  let to_super_cols l =
    List.map (fun r -> of_super_column r#grab_super_column) l
  in
    try
      let unmap = (M.find cf t.ks_rewrite).unmap in
      let h' = Hashtbl.create (Hashtbl.length h) in
        Hashtbl.iter (fun k v -> Hashtbl.add h' (unmap k) (to_super_cols v)) h;
        h'
    with Not_found -> Hashtbl.map to_super_cols h

let count t ?level ~cf ~key ?sc pred =
  t.ks_client#get_count
    (map_key t ~cf key) (column_parent cf ?sc) (slice_predicate pred) (clevel t level)

let get_range_slices t ?level ~cf ?sc pred range =
  let r = t.ks_client#get_range_slices
            (column_parent cf ?sc)
            (slice_predicate pred) (key_range t cf range) (clevel t level)
  in List.map (of_key_slice t cf) r

let get_range_superslices t ?level ~cf pred range =
  let r = t.ks_client#get_range_slices
            (column_parent cf)
            (slice_predicate pred) (key_range t cf range) (clevel t level)
  in List.map (of_key_super_slice t cf) r

let mk_timestamp = function
    None -> make_timestamp ()
  | Some t -> t

let mk_clock t = mk_clock (mk_timestamp t)

let make_column name ?timestamp value =
  { c_name=name; c_timestamp=mk_timestamp timestamp; c_value=value; }

let insert_column t ?level ~cf ~key ?sc ?timestamp col =
  t.ks_client#insert (map_key t ~cf key) (column_parent ?sc cf)
    (column (match timestamp with None -> col | Some t -> { col with c_timestamp = t }))
    (clevel t level)

let insert t ?level ~cf ~key ?sc ~name ?timestamp value =
  insert_column t ?level ~cf ~key ?sc (make_column name ?timestamp value)

let remove_key t ?level ~cf ?timestamp key =
  let cpath = new columnPath in
    cpath#set_column_family cf;
    t.ks_client#remove (map_key t ~cf key) cpath
      (mk_clock timestamp) (clevel t level)

let remove_column t ?level ~cf ~key ?sc ?timestamp name =
  t.ks_client#remove (map_key t ~cf key)
    (column_path ~cf ?sc name)
    (mk_clock timestamp) (clevel t level)

let remove_supercolumn t ?level ~cf ~key ?timestamp name =
  t.ks_client#remove (map_key t ~cf key)
    (supercolumn_path ~cf name)
    (mk_clock timestamp) (clevel t level)

let truncate t ~cf =
  t.ks_client#truncate cf

let make_deletion ?sc ?predicate timestamp =
  let r = new deletion in
    r#set_clock (mk_clock timestamp);
    Option.may r#set_super_column sc;
    Option.may r#set_predicate (Option.map slice_predicate predicate);
    r

let make_column_or_supercolumn ?col ?super () =
  let c = new columnOrSuperColumn in
    Option.may c#set_column (Option.map column col);
    Option.may c#set_super_column (Option.map supercolumn super);
    c

let mutation (m : mutation) =
  let r = new mutation in
    begin
      match m with
          `Insert col ->
            r#set_column_or_supercolumn (make_column_or_supercolumn ~col ())
        | `Insert_super super ->
            r#set_column_or_supercolumn (make_column_or_supercolumn ~super ())
        | `Delete (timestamp, what) ->
            r#set_deletion begin match what with
                `Key -> make_deletion timestamp
              | `Super_column sc ->
                  make_deletion ~sc timestamp
              | `Columns predicate -> make_deletion ~predicate timestamp
              | `Sub_columns (sc, predicate) ->
                  make_deletion ~sc ~predicate timestamp
            end
    end;
    r

let find_insert_default f h k =
  try
    Hashtbl.find h k
  with Not_found ->
    let v = f () in
      Hashtbl.add h k v;
      v

let batch_mutate t ?level l =
  let h = Hashtbl.create (List.length l) in
    List.iter
      (fun (key, l1) ->
         (* we rewrite the keys *)
         (* Hashtbl.iter will return the elements in reverse order if we
          * Hashtbl.add with the same key, so we reverse the lists first *)
         List.iter
           (fun (cf, muts) ->
              let key = map_key t cf key in
              let h1 = find_insert_default (fun () -> Hashtbl.create 13) h key in
                Hashtbl.add h1 cf (List.map mutation muts))
           (List.rev l1))
      l;
    t.ks_client#batch_mutate h (clevel t level)

let insert_supercolumn t ?level ~cf ~key ~name ?timestamp l =
  let timestamp = mk_timestamp timestamp in
  let columns =
    List.map
      (fun (n, v) -> { c_name = n; c_timestamp = timestamp; c_value = v }) l in
  let mutation = `Insert_super { sc_name = name; sc_columns = columns } in
    batch_mutate t ?level [key, [cf, [mutation]]]

module Batch =
struct
  type batch = { mutable ops : (string * (string * mutation list) list) list }

  let batch f =
    let b = { ops = [] } in
      f b;
      List.rev b.ops

  let batch_run ks ?level f = batch_mutate ks ?level (batch f)

  let add t op = t.ops <- op :: t.ops

  let insert t ~cf ~key ?sc ~name ?timestamp value =
    let column =
      { c_name = name; c_value = value; c_timestamp = mk_timestamp timestamp }
    in
      add t (key, [cf, [`Insert column]])

  let insert_supercolumn t ~cf ~key ~name ?timestamp l =
    let timestamp = mk_timestamp timestamp in
    let columns =
      List.map
        (fun (k, v) -> { c_name = k; c_value = v; c_timestamp = timestamp })
        l in
    let sc = { sc_name = name; sc_columns = columns } in
      add t (key, [cf, [`Insert_super sc]])

  let remove_key t ~cf ?timestamp key =
    add t (key, [cf, [`Delete (timestamp, `Key)]])

  let remove_column t ~cf ~key ?sc ?timestamp name =
    let what = match sc with
        None -> `Columns (`Columns [name])
      | Some sc -> `Sub_columns (sc, `Columns [name])
    in add t (key, [cf, [`Delete (timestamp, what)]])

  let remove_supercolumn t ~cf ~key ?timestamp name =
    add t (key, [cf, [`Delete (timestamp, `Super_column name)]])
end

module Typed =
struct
  type 'a column =
      {
        lev : level option; cf : string; name : string;
        of_s : string -> 'a; to_s : 'a -> string
      }

  type 'a subcolumn = 'a column

  let column ?level ~cf ~of_s ~to_s name =
    { lev = level; name = name; cf = cf; of_s = of_s; to_s = to_s }

  let clevel col = function
      None -> col.lev
    | Some _ as x -> x

  let subcolumn = column

  let get t ?level col ~key =
    col.of_s (get_value t ?level:(clevel col level)
                ~key ~cf:col.cf col.name)

  let get' t ?level ~sc col ~key =
    col.of_s (get_value t ?level:(clevel col level)
                ~key ~cf:col.cf ~sc col.name)

  let set t ?level col ~key ?timestamp x =
    insert t ?level:(clevel col level)
      ~key ~cf:col.cf ~name:col.name ?timestamp (col.to_s x)

  let set' t ?level ~sc col ~key ?timestamp x =
    insert t ?level:(clevel col level)
      ~key ~cf:col.cf ~name:col.name ~sc ?timestamp (col.to_s x)
end
