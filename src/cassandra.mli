(* Copyright (c) 2009 Mauricio Fernández <mfp@acm.org> *)

type cassandra_error =
    Field_empty of string
  | Transport_error of string
  | Protocol_error of string
  | Application_error of string
  | Unknown_error of exn * string

exception Cassandra_error of cassandra_error * string

type timestamp = Int64.t
type column = private { c_name : string; c_value : string; c_timestamp : timestamp; }
type supercolumn = private { sc_name : string; sc_columns : column list }

type level =
    [ `ZERO | `ONE | `QUORUM | `DCQUORUM | `DCQUORUMSYNC | `ALL | `ANY ]

type slice_predicate =
    [ `Columns of string list | `Column_range of string * string * bool * int ]

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
type key_rewriter

val make_timestamp : unit -> timestamp

val connect : host:string -> int -> connection
val disconnect : connection -> unit
val reconnect : ?force:bool -> connection -> unit
val valid_connection : connection -> bool

val key_rewriter :
  map:(string -> string) -> unmap:(string -> string) -> key_rewriter

val digest_rewriter : key_rewriter

(** @param rewrite_keys allows to specify a key rewriting function per column
  * family, which will be applied to the key(s) in all operations.
  *
  * Key rewriting can be useful if you want to use an order-preserving
  * partitioner but want the keys in some column families to be distributed
  * randomly. *)
val get_keyspace : connection -> ?level:level ->
  ?rewrite_keys:(string * key_rewriter) list -> string -> keyspace

val login : keyspace -> (string * string) list -> unit

val get : keyspace -> ?level:level ->
  cf:string -> key:string -> ?sc:string -> string -> column

val get_value : keyspace -> ?level:level ->
  cf:string -> key:string -> ?sc:string -> string -> string

val get' : keyspace -> ?level:level ->
  cf:string -> key:string -> string -> supercolumn

val get_supercolumn : keyspace -> ?level:level ->
  cf:string -> key:string -> string -> supercolumn

val get_slice : keyspace -> ?level:level ->
  cf:string -> key:string -> ?sc:string -> slice_predicate -> column list

val get_superslice : keyspace -> ?level:level ->
  cf:string -> key:string -> slice_predicate -> supercolumn list

val multiget_slice : keyspace -> ?level:level ->
  cf:string -> string list -> ?sc:string -> slice_predicate ->
  (string, column list) Hashtbl.t

val multiget_superslice : keyspace -> ?level:level ->
  cf:string -> string list -> slice_predicate ->
  (string, supercolumn list) Hashtbl.t

val count : keyspace -> ?level:level ->
  cf:string -> key:string -> ?sc:string -> unit -> int

val get_range_slices : keyspace -> ?level:level ->
  cf:string -> ?sc:string -> slice_predicate -> key_range -> key_slice list

val get_range_superslices : keyspace -> ?level:level ->
  cf:string -> slice_predicate -> key_range -> key_superslice list

val insert : keyspace -> ?level:level ->
  cf:string -> key:string -> ?sc:string -> name:string ->
  ?timestamp:timestamp -> string -> unit

val insert_supercolumn : keyspace -> ?level:level ->
  cf:string -> key:string -> name:string -> ?timestamp:timestamp ->
  (string * string) list -> unit

(** Use the timestamp in [column] unless another one is specified. *)
val insert_column : keyspace -> ?level:level ->
  cf:string -> key:string -> ?sc:string -> ?timestamp:timestamp -> column -> unit

val remove_key : keyspace -> ?level:level ->
  cf:string -> ?timestamp:timestamp -> string -> unit

val remove_column : keyspace -> ?level:level ->
  cf:string -> key:string -> ?sc:string -> ?timestamp:timestamp -> string -> unit

val remove_supercolumn : keyspace -> ?level:level ->
  cf:string -> key:string -> ?timestamp:timestamp -> string -> unit

(** (key * (column_family * mutation list) list) list *)
val batch_mutate : keyspace -> ?level:level ->
  (string * (string * mutation list) list) list -> unit

(** {2 Batch operations } *)

module Batch :
sig
  type batch

  val batch : (batch -> unit) ->
    (string * (string * mutation list) list) list

  val batch_run : keyspace -> ?level:level -> (batch -> unit) -> unit

  val insert : batch ->
    cf:string -> key:string -> ?sc:string -> name:string ->
    ?timestamp:timestamp -> string -> unit

  val insert_supercolumn : batch ->
    cf:string -> key:string -> name:string -> ?timestamp:timestamp ->
    (string * string) list -> unit

  val remove_key : batch ->
    cf:string -> ?timestamp:timestamp -> string -> unit

  val remove_column : batch ->
    cf:string -> key:string -> ?sc:string -> ?timestamp:timestamp -> string -> unit

  val remove_supercolumn : batch ->
    cf:string -> key:string -> ?timestamp:timestamp -> string -> unit
end


module Typed :
sig
  type 'a column
  type 'a subcolumn

  val column :
    ?level:level -> cf:string ->
    of_s:(string -> 'a) -> to_s:('a -> string) -> string -> 'a column

  val subcolumn :
    ?level:level -> cf:string ->
    of_s:(string -> 'a) -> to_s:('a -> string) -> string -> 'a subcolumn

  val get : keyspace -> ?level:level -> 'a column -> key:string -> 'a

  val set : keyspace -> ?level:level ->
    'a column -> key:string -> ?timestamp:timestamp -> 'a -> unit

  val get' : keyspace -> ?level:level ->
    sc:string -> 'a subcolumn -> key:string -> 'a

  val set' : keyspace -> ?level:level ->
    sc:string -> 'a subcolumn -> key:string -> ?timestamp:timestamp -> 'a -> unit
end
