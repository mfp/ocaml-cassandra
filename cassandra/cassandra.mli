
type timestamp = Int64.t
type column = { c_name : string; c_value : string; c_timestamp : timestamp; }
type supercolumn = { sc_name : string; sc_columns : column list }

type column_path =
    [`C of string * string | `SC of string * string * string]

type supercolumn_path = string * string

type column_parent = [`CF of string | `SC of string * string]

type consistency_level =
    [ `ZERO | `ONE | `QUORUM | `DCQUORUM | `DCQUORUMSYNC | `ALL | `ANY ]

type slice_predicate =
    [ `Columns of string list | `Range of string * string * bool * int ]

type key_range =
    [ `Key of string * string * int | `Token of string * string * int ]

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

type connection
type keyspace

val connect : host:string -> int -> connection
val disconnect : connection -> unit
val reconnect : connection -> unit
val valid_connection : connection -> bool

val get_keyspace : connection -> string -> keyspace

val get : keyspace ->
  key:string -> ?consistency_level:consistency_level ->
  column_path -> column

val get_column : keyspace -> ?consistency_level:consistency_level ->
  key:string -> cf:string -> string -> column

val get_subcolumn : keyspace -> ?consistency_level:consistency_level ->
  key:string -> cf:string -> string -> string -> column

val get' : keyspace ->
  key:string -> ?consistency_level:consistency_level ->
  supercolumn_path -> supercolumn

val get_supercolumn : keyspace ->
  key:string -> ?consistency_level:consistency_level ->
  supercolumn_path -> supercolumn

val get_slice : keyspace ->
  key:string -> ?consistency_level:consistency_level ->
  parent:column_parent -> slice_predicate -> column list

val get_column_slice : keyspace ->
  key:string -> ?consistency_level:consistency_level ->
  cf:string -> slice_predicate -> column list

val get_subcolumn_slice : keyspace ->
  key:string -> ?consistency_level:consistency_level ->
  cf:string -> supercolumn:string -> slice_predicate -> column list

val multiget_slice : keyspace ->
  string list -> ?consistency_level:consistency_level ->
  parent:column_parent -> slice_predicate -> (string, column list) Hashtbl.t

val multiget_column_slice : keyspace ->
  string list -> ?consistency_level:consistency_level ->
  cf:string -> slice_predicate -> (string, column list) Hashtbl.t

val multiget_subcolumn_slice : keyspace ->
  string list -> ?consistency_level:consistency_level ->
  cf:string -> supercolumn:string -> slice_predicate ->
  (string, column list) Hashtbl.t

val count : keyspace ->
  key:string -> ?consistency_level:consistency_level ->
  column_parent -> int

val count_columns : keyspace ->
  key:string -> ?consistency_level:consistency_level ->
  string -> int

val count_subcolumns : keyspace ->
  key:string -> ?consistency_level:consistency_level ->
  cf:string -> string -> int

val get_range_slices : keyspace ->
  parent:column_parent ->
  ?consistency_level:consistency_level -> slice_predicate ->
  key_range -> key_slice list

val insert : keyspace ->
  key:string -> ?consistency_level:consistency_level ->
  column_path -> timestamp -> string -> unit

val insert_column : keyspace ->
  key:string -> ?consistency_level:consistency_level ->
  cf:string -> name:string -> timestamp -> string -> unit

val insert_subcolumn : keyspace ->
  key:string -> ?consistency_level:consistency_level ->
  cf:string -> supercolumn:string -> name:string ->
  timestamp -> string -> unit

val remove_key : keyspace ->
  key:string -> ?consistency_level:consistency_level ->
  timestamp -> string -> unit

val remove_column : keyspace ->
  key:string -> ?consistency_level:consistency_level ->
  timestamp -> column_path -> unit

val remove_supercolumn : keyspace ->
  key:string -> ?consistency_level:consistency_level ->
  timestamp -> supercolumn_path -> unit

(** (key * (column_family * mutation list) list) list *)
val batch_mutate : keyspace ->
  ?consistency_level:consistency_level ->
  (string * (string * mutation list) list) list -> unit
