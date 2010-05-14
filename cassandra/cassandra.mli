
type timestamp = Int64.t
type column = { c_name : string; c_value : string; c_timestamp : timestamp; }
type super_column = { sc_name : string; sc_columns : column list }

type column_path =
    string * [`Column of string | `Subcolumn of string * string]

type super_column_path = string * string

type column_parent = string
type column_parent' = column_parent * string

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

type connection

val connect : keyspace:string -> host:string -> int -> connection
val disconnect : connection -> unit
val reconnect : connection -> unit
val valid_connection : connection -> bool

val get : connection ->
  key:string -> ?consistency_level:consistency_level ->
  column_path -> column

val get' : connection ->
  key:string -> ?consistency_level:consistency_level ->
  super_column_path -> super_column

val get_slice : connection ->
  key:string -> ?consistency_level:consistency_level ->
  parent:column_parent -> slice_predicate -> column list

val multiget_slice : connection ->
  string list -> ?consistency_level:consistency_level ->
  parent:column_parent -> slice_predicate -> (string, column list) Hashtbl.t

val count : connection ->
  key:string -> ?consistency_level:consistency_level ->
  column_parent -> int

val get_range_slices : connection ->
  parent:column_parent ->
  ?consistency_level:consistency_level -> slice_predicate ->
  key_range -> key_slice list

val insert : connection ->
  key:string -> ?consistency_level:consistency_level ->
  column_path -> timestamp -> string -> unit

val remove_key : connection ->
  key:string -> ?consistency_level:consistency_level ->
  timestamp -> string -> unit

val remove_column : connection ->
  key:string -> ?consistency_level:consistency_level ->
  timestamp -> column_path -> unit

val remove_super_column : connection ->
  key:string -> ?consistency_level:consistency_level ->
  timestamp -> super_column_path -> unit

(** (key * (column_family * mutation list) list) list *)
val batch_mutate : connection ->
  ?consistency_level:consistency_level ->
  (string * (string * mutation list) list) list -> unit
