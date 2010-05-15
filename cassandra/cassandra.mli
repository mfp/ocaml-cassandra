
type timestamp = Int64.t
type column = private { c_name : string; c_value : string; c_timestamp : timestamp; }
type supercolumn = private { sc_name : string; sc_columns : column list }

type consistency_level =
    [ `ZERO | `ONE | `QUORUM | `DCQUORUM | `DCQUORUMSYNC | `ALL | `ANY ]

type slice_predicate =
    [ `Columns of string list | `Range of string * string * bool * int ]

type key_range =
    [ `Key of string * string * int | `Token of string * string * int ]

type key_slice = string * column list
type key_superslice = string * supercolumn list

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
  cf:string -> ?supercolumn:string -> string -> column

val get_value : keyspace ->
  key:string -> ?consistency_level:consistency_level ->
  cf:string -> ?supercolumn:string -> string -> string

val get' : keyspace ->
  key:string -> ?consistency_level:consistency_level ->
  cf:string -> string -> supercolumn

val get_supercolumn : keyspace ->
  key:string -> ?consistency_level:consistency_level ->
  cf:string -> string -> supercolumn

val get_slice : keyspace ->
  key:string -> ?consistency_level:consistency_level ->
  cf:string -> ?supercolumn:string -> slice_predicate -> column list

val get_superslice : keyspace ->
  key:string -> ?consistency_level:consistency_level ->
  cf:string -> slice_predicate -> supercolumn list

val multiget_slice : keyspace ->
  string list -> ?consistency_level:consistency_level ->
  cf:string -> ?supercolumn:string -> slice_predicate ->
  (string, column list) Hashtbl.t

val multiget_superslice : keyspace ->
  string list -> ?consistency_level:consistency_level ->
  cf:string -> slice_predicate ->
  (string, supercolumn list) Hashtbl.t

val count : keyspace ->
  key:string -> ?consistency_level:consistency_level ->
  cf:string -> ?supercolumn:string -> unit -> int

val get_range_slices : keyspace ->
  cf:string -> ?supercolumn:string ->
  ?consistency_level:consistency_level -> slice_predicate ->
  key_range -> key_slice list

val get_range_superslices : keyspace ->
  cf:string ->
  ?consistency_level:consistency_level -> slice_predicate ->
  key_range -> key_superslice list

val insert : keyspace ->
  key:string -> ?consistency_level:consistency_level ->
  cf:string -> ?supercolumn:string -> name:string -> timestamp -> string -> unit

val insert_column : keyspace ->
  key:string -> ?consistency_level:consistency_level ->
  cf:string -> ?supercolumn:string -> ?timestamp:timestamp -> column -> unit

val remove_key : keyspace ->
  key:string -> ?consistency_level:consistency_level ->
  timestamp -> string -> unit

val remove_column : keyspace ->
  key:string -> ?consistency_level:consistency_level ->
  cf:string -> ?supercolumn:string -> timestamp -> string -> unit

val remove_supercolumn : keyspace ->
  key:string -> ?consistency_level:consistency_level ->
  cf:string -> timestamp -> string -> unit

(** (key * (column_family * mutation list) list) list *)
val batch_mutate : keyspace ->
  ?consistency_level:consistency_level ->
  (string * (string * mutation list) list) list -> unit
