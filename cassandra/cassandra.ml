open ExtList
open ExtHashtbl
open Cassandra_thrift
open Cassandra_types

type timestamp = Int64.t
type column = { c_name : string; c_value : string; c_timestamp : timestamp; }
type super_column = { sc_name : string; sc_columns : column list }

type column_path = string * [`Column of string | `Subcolumn of string * string]

type super_column_path = string * string

type column_parent = [`CF of string | `SC of string * string]

type consistency_level =
    [ `ZERO | `ONE | `QUORUM | `DCQUORUM | `DCQUORUMSYNC | `ALL | `ANY ]

type slice_predicate =
    [ `Columns of string list | `Range of string * string * bool * int ]

type key_range =
    [ `Key_range of string * string | `Token_range of string * string ] * int

type key_slice = string * column list
type key_slice' = string * super_column list

type mutation =
    [
      `Delete of timestamp *
        [ `Key | `Super_column of string | `Columns of slice_predicate
        | `Sub_columns of string * slice_predicate ]
    | `Insert of column
    | `Insert_super of super_column
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

let super_column c =
  let r = new superColumn in
    r#set_name c.sc_name;
    r#set_columns (List.map column c.sc_columns);
    r

let of_super_column c =
  { sc_name = c#grab_name; sc_columns = List.map of_column c#grab_columns; }

let column_path (family, path) =
  let r = new columnPath in
    r#set_column_family family;
    begin
      match path with
          `Column n -> r#set_column n
        | `Subcolumn (sup, sub) ->
            r#set_super_column sup;
            r#set_column sub
    end;
    r

let super_column_path (family, sup) =
  let r = new columnPath in
    r#set_column_family family;
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
  List.filter_map (fun r -> Option.map of_super_column r#get_super_column) l

let key_range (r, count) =
  let o = new keyRange in
    begin
      o#set_count count;
      match r with
          `Key_range (start, stop) -> o#set_start_key start;
                                      o#set_end_key stop
        | `Token_range (start, stop) -> o#set_start_token start;
                                        o#set_end_token stop
    end;
    o

let of_key_slice r = (r#grab_key, get_columns r#grab_columns)
let of_key_slice' r = (r#grab_key, get_columns' r#grab_columns)

let get t ~key ?consistency_level cpath =
  let r = t.ks_client#get t.ks_name
            key (column_path cpath) (clevel consistency_level)
  in of_column r#grab_column

let get_column t ?consistency_level ~key ~cf column =
  get t ~key ?consistency_level (cf, `Column column)

let get_subcolumn t ?consistency_level ~key ~cf supercol subcol =
  get t ~key ?consistency_level (cf, `Subcolumn (supercol, subcol))

let get' t ~key ?consistency_level cpath =
  let r = t.ks_client#get t.ks_name key (super_column_path cpath)
            (clevel consistency_level)
  in of_super_column r#grab_super_column

let get_supercolumn = get'

let get_slice t ~key ?consistency_level ~parent pred =
  let cols =
    t.ks_client#get_slice t.ks_name key
      (column_parent parent) (slice_predicate pred) (clevel consistency_level)
  in get_columns cols

let get_column_slice t ~key ?consistency_level ~family pred =
  get_slice t ~key ?consistency_level ~parent:(`CF family) pred

let get_subcolumn_slice t ~key ?consistency_level ~family ~supercolumn pred =
  get_slice t ~key ?consistency_level ~parent:(`SC (family, supercolumn)) pred

let multiget_slice t keys ?consistency_level ~parent pred =
  let h =
    t.ks_client#multiget_slice t.ks_name keys
      (column_parent parent) (slice_predicate pred) (clevel consistency_level)
  in Hashtbl.map (List.map (fun r -> of_column r#grab_column)) h

let multiget_column_slice t keys ?consistency_level ~family pred =
  multiget_slice t keys ?consistency_level ~parent:(`CF family) pred

let multiget_subcolumn_slice
      t keys ?consistency_level ~family ~supercolumn pred =
  multiget_slice t keys ?consistency_level ~parent:(`SC (family, supercolumn)) pred

let count t ~key ?consistency_level parent =
  t.ks_client#get_count t.ks_name
    key (column_parent parent) (clevel consistency_level)

let count_columns t ~key ?consistency_level family =
  count t ~key ?consistency_level (`CF family)

let count_subcolumns t ~key ?consistency_level ~family supercol =
  count t ~key ?consistency_level (`SC (family, supercol))

let get_range_slices
      t ~parent ?consistency_level pred range =
  let r = t.ks_client#get_range_slices t.ks_name (column_parent parent)
            (slice_predicate pred) (key_range range) (clevel consistency_level)
  in List.map of_key_slice r

let insert t ~key ?consistency_level cpath timestamp value =
  t.ks_client#insert t.ks_name key
    (column_path cpath) value timestamp (clevel consistency_level)

let insert_column t ~key ?consistency_level ~family ~name timestamp value =
  insert t ~key ?consistency_level (family, `Column name) timestamp value

let insert_subcolumn
      t ~key ?consistency_level ~family ~supercolumn ~name timestamp value =
  insert t ~key ?consistency_level
    (family, `Subcolumn (supercolumn, name)) timestamp value

let make_column_path ?super ?column family =
  let r = new columnPath in
    r#set_column_family family;
    Option.may r#set_super_column super;
    Option.may r#set_column column;
    r

let remove_key t ~key ?consistency_level timestamp column_family =
  t.ks_client#remove t.ks_name key (make_column_path column_family) timestamp
    (clevel consistency_level)

let remove_column t ~key ?consistency_level timestamp cpath =
  t.ks_client#remove t.ks_name key (column_path cpath) timestamp
    (clevel consistency_level)

let remove_super_column t ~key ?consistency_level timestamp path =
  t.ks_client#remove t.ks_name key (super_column_path path) timestamp
    (clevel consistency_level)

let make_deletion ?super_column ?predicate timestamp =
  let r = new deletion in
    Option.may r#set_super_column super_column;
    Option.may r#set_predicate (Option.map slice_predicate predicate);
    r

let make_column_or_supercolumn ?col ?super () =
  let c = new columnOrSuperColumn in
    Option.may c#set_column (Option.map column col);
    Option.may c#set_super_column (Option.map super_column super);
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
              | `Super_column super_column ->
                  make_deletion ~super_column timestamp
              | `Columns predicate -> make_deletion ~predicate timestamp
              | `Sub_columns (super_column, predicate) ->
                  make_deletion ~super_column ~predicate timestamp
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

