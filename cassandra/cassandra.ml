open ExtList
open ExtHashtbl
open Cassandra_thrift
open Cassandra_types

type timestamp = Int64.t
type column = { c_name : string; c_value : string; c_timestamp : timestamp; }
type supercolumn = { sc_name : string; sc_columns : column list }

type column_path =
    [`C of string * string | `SC of string * string * string]

type column_parent = [`CF of string | `SC of string * string]

type consistency_level =
    [ `ZERO | `ONE | `QUORUM | `DCQUORUM | `DCQUORUMSYNC | `ALL | `ANY ]

type slice_predicate =
    [ `Columns of string list | `Range of string * string * bool * int ]

type key_range =
    [ `Key of string * string * int | `Token of string * string * int]

type key_slice = string * column list
type key_slice' = string * supercolumn list

type mutation =
    [
      `Delete of timestamp *
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
}

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

let get_keyspace t name = { ks_name = name; ks_client = t.client; }

open ConsistencyLevel

let clevel = function
    None -> ONE
  | Some `ZERO -> ZERO
  | Some `ONE -> ONE
  | Some `QUORUM -> QUORUM
  | Some `DCQUORUM -> DCQUORUM
  | Some `DCQUORUMSYNC -> DCQUORUMSYNC
  | Some `ALL -> ALL
  | Some `ANY -> ANY

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

let of_supercolumn c =
  { sc_name = c#grab_name; sc_columns = List.map of_column c#grab_columns; }

let column_path ~cf ?supercolumn c =
  let r = new columnPath in
    r#set_column_family cf;
    Option.may r#set_super_column supercolumn;
    r#set_column c;
    r

let supercolumn_path ~cf sup =
  let r = new columnPath in
    r#set_column_family cf;
    r#set_super_column sup;
    r

let column_parent p =
  let o = new columnParent in
    begin match p with
        `CF s -> o#set_column_family s
      | `SC (cf, c) -> o#set_column_family cf; o#set_super_column c
    end;
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

let get_columns' l =
  List.filter_map (fun r -> Option.map of_supercolumn r#get_supercolumn) l

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
let of_key_slice' r = (r#grab_key, get_columns' r#grab_columns)

let get t ~key ?consistency_level ~cf ?supercolumn column =
  let r = t.ks_client#get t.ks_name
            key (column_path ~cf ?supercolumn column) (clevel consistency_level)
  in of_column r#grab_column

let get' t ~key ?consistency_level ~cf name =
  let r = t.ks_client#get t.ks_name key (supercolumn_path ~cf name)
            (clevel consistency_level)
  in of_supercolumn r#grab_super_column

let get_supercolumn = get'

let get_slice t ~key ?consistency_level ~parent pred =
  let cols =
    t.ks_client#get_slice t.ks_name key
      (column_parent parent) (slice_predicate pred) (clevel consistency_level)
  in get_columns cols

let get_column_slice t ~key ?consistency_level ~cf pred =
  get_slice t ~key ?consistency_level ~parent:(`CF cf) pred

let get_subcolumn_slice t ~key ?consistency_level ~cf ~supercolumn pred =
  get_slice t ~key ?consistency_level ~parent:(`SC (cf, supercolumn)) pred

let multiget_slice t keys ?consistency_level ~parent pred =
  let h =
    t.ks_client#multiget_slice t.ks_name keys
      (column_parent parent) (slice_predicate pred) (clevel consistency_level)
  in Hashtbl.map (List.map (fun r -> of_column r#grab_column)) h

let multiget_column_slice t keys ?consistency_level ~cf pred =
  multiget_slice t keys ?consistency_level ~parent:(`CF cf) pred

let multiget_subcolumn_slice
      t keys ?consistency_level ~cf ~supercolumn pred =
  multiget_slice t keys ?consistency_level ~parent:(`SC (cf, supercolumn)) pred

let count t ~key ?consistency_level parent =
  t.ks_client#get_count t.ks_name
    key (column_parent parent) (clevel consistency_level)

let count_columns t ~key ?consistency_level cf =
  count t ~key ?consistency_level (`CF cf)

let count_subcolumns t ~key ?consistency_level ~cf supercol =
  count t ~key ?consistency_level (`SC (cf, supercol))

let get_range_slices
      t ~parent ?consistency_level pred range =
  let r = t.ks_client#get_range_slices t.ks_name (column_parent parent)
            (slice_predicate pred) (key_range range) (clevel consistency_level)
  in List.map of_key_slice r

let insert t ~key ?consistency_level ~cf ?supercolumn ~name timestamp value =
  t.ks_client#insert t.ks_name key
    (column_path ~cf ?supercolumn name) value timestamp (clevel consistency_level)

let make_column_path ?super ?column cf =
  let r = new columnPath in
    r#set_column_family cf;
    Option.may r#set_super_column super;
    Option.may r#set_column column;
    r

let remove_key t ~key ?consistency_level timestamp column_family =
  t.ks_client#remove t.ks_name key (make_column_path column_family) timestamp
    (clevel consistency_level)

let remove_column t ~key ?consistency_level ~cf ?supercolumn timestamp name =
  t.ks_client#remove t.ks_name key
    (column_path ~cf ?supercolumn name) timestamp (clevel consistency_level)

let remove_supercolumn t ~key ?consistency_level ~cf timestamp name =
  t.ks_client#remove t.ks_name key
    (supercolumn_path ~cf name) timestamp (clevel consistency_level)

let make_deletion ?supercolumn ?predicate timestamp =
  let r = new deletion in
    Option.may r#set_super_column supercolumn;
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
              | `Super_column supercolumn ->
                  make_deletion ~supercolumn timestamp
              | `Columns predicate -> make_deletion ~predicate timestamp
              | `Sub_columns (supercolumn, predicate) ->
                  make_deletion ~supercolumn ~predicate timestamp
            end
    end;
    r

let batch_mutate t ?consistency_level l =
  let h = Hashtbl.create (List.length l) in
    List.iter
      (fun (key, l1) ->
         let h1 = Hashtbl.create (List.length l1) in
           Hashtbl.add h key h1;
           List.iter
             (fun (cf, muts) -> Hashtbl.add h1 cf (List.map mutation muts)) l1)
      l;
    t.ks_client#batch_mutate t.ks_name h (clevel consistency_level)

