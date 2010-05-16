
type timestamp = Int64.t
type column = private { c_name : string; c_value : string; c_timestamp : timestamp; }
type supercolumn = private { sc_name : string; sc_columns : column list }

type level =
    [ `ZERO | `ONE | `QUORUM | `DCQUORUM | `DCQUORUMSYNC | `ALL | `ANY ]

type slice_predicate =
    [ `Columns of string list | `Range of string * string * bool * int ]

type key_range =
    [ `Key of string * string * int | `Token of string * string * int ]

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

type connection
type keyspace

val make_timestamp : unit -> timestamp

val connect : host:string -> int -> connection
val disconnect : connection -> unit
val reconnect : connection -> unit
val valid_connection : connection -> bool

val get_keyspace : connection -> ?level:level -> string -> keyspace

val get : keyspace ->
  key:string -> ?level:level ->
  cf:string -> ?supercolumn:string -> string -> column

val get_value : keyspace ->
  key:string -> ?level:level ->
  cf:string -> ?supercolumn:string -> string -> string

val get' : keyspace ->
  key:string -> ?level:level ->
  cf:string -> string -> supercolumn

val get_supercolumn : keyspace ->
  key:string -> ?level:level ->
  cf:string -> string -> supercolumn

val get_slice : keyspace ->
  key:string -> ?level:level ->
  cf:string -> ?supercolumn:string -> slice_predicate -> column list

val get_superslice : keyspace ->
  key:string -> ?level:level ->
  cf:string -> slice_predicate -> supercolumn list

val multiget_slice : keyspace ->
  string list -> ?level:level ->
  cf:string -> ?supercolumn:string -> slice_predicate ->
  (string, column list) Hashtbl.t

val multiget_superslice : keyspace ->
  string list -> ?level:level ->
  cf:string -> slice_predicate ->
  (string, supercolumn list) Hashtbl.t

val count : keyspace ->
  key:string -> ?level:level ->
  cf:string -> ?supercolumn:string -> unit -> int

val get_range_slices : keyspace ->
  cf:string -> ?supercolumn:string ->
  ?level:level -> slice_predicate ->
  key_range -> key_slice list

val get_range_superslices : keyspace ->
  cf:string ->
  ?level:level -> slice_predicate ->
  key_range -> key_superslice list

val insert : keyspace ->
  key:string -> ?level:level ->
  cf:string -> ?supercolumn:string -> name:string ->
  ?timestamp:timestamp -> string -> unit

val insert_supercolumn : keyspace ->
  key:string -> ?level:level ->
  cf:string -> name:string -> ?timestamp:timestamp ->
  (string * string) list -> unit

(** Use the timestamp in [column] unless another one is specified. *)
val insert_column : keyspace ->
  key:string -> ?level:level ->
  cf:string -> ?supercolumn:string -> ?timestamp:timestamp -> column -> unit

val remove_key : keyspace ->
  key:string -> ?level:level ->
  ?timestamp:timestamp -> string -> unit

val remove_column : keyspace ->
  key:string -> ?level:level ->
  cf:string -> ?supercolumn:string -> ?timestamp:timestamp -> string -> unit

val remove_supercolumn : keyspace ->
  key:string -> ?level:level ->
  cf:string -> ?timestamp:timestamp -> string -> unit

(** (key * (column_family * mutation list) list) list *)
val batch_mutate : keyspace ->
  ?level:level ->
  (string * (string * mutation list) list) list -> unit

module Typed :
sig
  type 'a column
  type 'a subcolumn

  val column :
    ?level:level ->
    cf:string -> of_s:(string -> 'a) -> to_s:('a -> string) ->
    string -> 'a column

  val subcolumn :
    ?level:level ->
    cf:string -> of_s:(string -> 'a) -> to_s:('a -> string) ->
    string -> 'a subcolumn

  val get : keyspace -> ?level:level ->
    key:string -> 'a column -> 'a

  val set : keyspace -> ?level:level ->
    key:string -> 'a column -> ?timestamp:timestamp -> 'a -> unit

  val get' : keyspace -> ?level:level ->
    key:string -> supercolumn:string -> 'a subcolumn -> 'a

  val set' : keyspace -> ?level:level ->
    key:string -> supercolumn:string -> 'a subcolumn ->
    ?timestamp:timestamp -> 'a -> unit
end
