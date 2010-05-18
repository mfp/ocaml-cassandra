(* Copyright (c) 2009 Mauricio Fernández <mfp@acm.org> *)

IFDEF EXTLIB THEN
  open ExtList
  open ExtHashtbl
ELSE
  module Option = BatOption
  module Hashtbl =
  struct
    include BatHashtbl
    let map f h = map (fun k v -> f v) h
  end
  module List = struct include List include BatList end
ENDIF

open Cassandra_thrift
open Cassandra_types

type timestamp = Int64.t
type column = { c_name : string; c_value : string; c_timestamp : timestamp; }
type supercolumn = { sc_name : string; sc_columns : column list }

type level = [ `ZERO | `ONE | `QUORUM | `DCQUORUM | `DCQUORUMSYNC | `ALL | `ANY ]

type slice_predicate =
    [ `Columns of string list | `Range of string * string * bool * int ]

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

type keyspace = {
  ks_name : string;
  ks_client : Cassandra.client;
  ks_level : level;
}

let make_timestamp () = Int64.of_float (1e6 *. Unix.gettimeofday ())

let connect ~host port =
  let tx = new TSocket.t host port in
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

let get_keyspace t ?(level = `ONE) name =
  { ks_name = name; ks_client = t.client; ks_level = level; }

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

let column c =
  let r = new column in
    r#set_name c.c_name;
    r#set_value c.c_value;
    r#set_timestamp c.c_timestamp;
    r

let of_column c =
  {
    c_name = c#grab_name; c_value = c#grab_value;
    c_timestamp = c#grab_timestamp;
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
      | `Range (start, finish, reversed, count) ->
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

let key_range r =
  let o = new keyRange in
    begin
      match r with
          `Key (start, stop, count) -> o#set_start_key start;
                                       o#set_end_key stop;
                                       o#set_count count
        | `Token (start, stop, count) -> o#set_start_token start;
                                         o#set_end_token stop;
                                         o#set_count count
    end;
    o

let of_key_slice r = (r#grab_key, get_columns r#grab_columns)
let of_key_super_slice r = (r#grab_key, get_supercolumns r#grab_columns)

let get t ?level ~cf ~key ?sc column =
  let r = t.ks_client#get t.ks_name
            key (column_path ~cf ?sc column) (clevel t level)
  in of_column r#grab_column

let get_value t ?level ~cf ~key ?sc col =
  (get t ~key ?level ~cf ?sc col).c_value

let get' t ?level ~cf ~key name =
  let r = t.ks_client#get t.ks_name key (supercolumn_path ~cf name)
            (clevel t level)
  in of_super_column r#grab_super_column

let get_supercolumn = get'

let get_slice t ?level ~cf ~key ?sc pred =
  let cols =
    t.ks_client#get_slice t.ks_name key
      (column_parent cf ?sc)
      (slice_predicate pred) (clevel t level)
  in get_columns cols

let get_superslice t ?level ~cf ~key pred =
  let cols =
    t.ks_client#get_slice t.ks_name key
      (column_parent cf) (slice_predicate pred) (clevel t level)
  in get_supercolumns cols

let multiget_slice t ?level ~cf keys ?sc pred =
  let h =
    t.ks_client#multiget_slice t.ks_name keys
      (column_parent cf ?sc)
      (slice_predicate pred) (clevel t level)
  in Hashtbl.map (List.map (fun r -> of_column r#grab_column)) h

let multiget_superslice t ?level ~cf keys pred =
  let h =
    t.ks_client#multiget_slice t.ks_name keys
      (column_parent cf) (slice_predicate pred) (clevel t level)
  in Hashtbl.map (List.map (fun r -> of_super_column r#grab_super_column)) h

let count t ?level ~cf ~key ?sc () =
  t.ks_client#get_count t.ks_name
    key (column_parent cf ?sc) (clevel t level)

let get_range_slices t ?level ~cf ?sc pred range =
  let r = t.ks_client#get_range_slices t.ks_name
            (column_parent cf ?sc)
            (slice_predicate pred) (key_range range) (clevel t level)
  in List.map of_key_slice r

let get_range_superslices t ?level ~cf pred range =
  let r = t.ks_client#get_range_slices t.ks_name
            (column_parent cf)
            (slice_predicate pred) (key_range range) (clevel t level)
  in List.map of_key_super_slice r

let mk_timestamp = function
    None -> make_timestamp ()
  | Some t -> t

let insert t ?level ~cf ~key ?sc ~name ?timestamp value =
  t.ks_client#insert t.ks_name key (column_path ~cf ?sc name)
    value (mk_timestamp timestamp) (clevel t level)

let insert_column t ?level ~cf ~key ?sc ?timestamp column =
  insert t ~key ?level ~cf ?sc
    ~name:column.c_name
    ~timestamp:(Option.default column.c_timestamp timestamp)
    column.c_value

let remove_key t ?level ~cf ?timestamp key =
  let cpath = new columnPath in
    cpath#set_column_family cf;
    t.ks_client#remove t.ks_name key cpath
      (mk_timestamp timestamp) (clevel t level)

let remove_column t ?level ~cf ~key ?sc ?timestamp name =
  t.ks_client#remove t.ks_name key
    (column_path ~cf ?sc name)
    (mk_timestamp timestamp) (clevel t level)

let remove_supercolumn t ?level ~cf ~key ?timestamp name =
  t.ks_client#remove t.ks_name key
    (supercolumn_path ~cf name)
    (mk_timestamp timestamp) (clevel t level)

let make_deletion ?sc ?predicate timestamp =
  let r = new deletion in
    r#set_timestamp (mk_timestamp timestamp);
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

let batch_mutate t ?level l =
  let h = Hashtbl.create (List.length l) in
    List.iter
      (fun (key, l1) ->
         let h1 = Hashtbl.create (List.length l1) in
           Hashtbl.add h key h1;
           List.iter
             (fun (cf, muts) -> Hashtbl.add h1 cf (List.map mutation muts)) l1)
      l;
    t.ks_client#batch_mutate t.ks_name h (clevel t level)

let insert_supercolumn t ?level ~cf ~key ~name ?timestamp l =
  let timestamp = mk_timestamp timestamp in
  let columns =
    List.map
      (fun (n, v) -> { c_name = n; c_timestamp = timestamp; c_value = v }) l in
  let mutation = `Insert_super { sc_name = name; sc_columns = columns } in
    batch_mutate t ?level [key, [cf, [mutation]]]

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
