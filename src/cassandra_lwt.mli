(* Copyright (c) 2009 Mauricio Fernández <mfp@acm.org> *)

open Cassandra

type conn_pool

val make_pool : (string * int) list ->
  ?credentials:(string * string) list ->
  ?level:level -> ?rewrite_keys:(string * key_rewriter) list ->
  keyspace:string -> int -> conn_pool

val get : conn_pool -> ?level:level ->
  cf:string -> key:string -> ?sc:string -> string -> column option Lwt.t

val get_value : conn_pool -> ?level:level ->
  cf:string -> key:string -> ?sc:string -> string -> string option Lwt.t

val get' : conn_pool -> ?level:level ->
  cf:string -> key:string -> string -> supercolumn option Lwt.t

val get_supercolumn : conn_pool -> ?level:level ->
  cf:string -> key:string -> string -> supercolumn option Lwt.t

val get_slice : conn_pool -> ?level:level ->
  cf:string -> key:string -> ?sc:string -> slice_predicate -> column list Lwt.t

val get_superslice : conn_pool -> ?level:level ->
  cf:string -> key:string -> slice_predicate -> supercolumn list Lwt.t

val multiget_slice : conn_pool -> ?level:level ->
  cf:string -> string list -> ?sc:string -> slice_predicate ->
  (string, column list) Hashtbl.t Lwt.t

val multiget_superslice : conn_pool -> ?level:level ->
  cf:string -> string list -> slice_predicate ->
  (string, supercolumn list) Hashtbl.t Lwt.t

val count : conn_pool -> ?level:level ->
  cf:string -> key:string -> ?sc:string -> slice_predicate -> int Lwt.t

val get_range_slices : conn_pool -> ?level:level ->
  cf:string -> ?sc:string -> slice_predicate -> key_range -> key_slice list Lwt.t

val get_range_superslices : conn_pool -> ?level:level ->
  cf:string -> slice_predicate -> key_range -> key_superslice list Lwt.t

val insert : conn_pool -> ?level:level ->
  cf:string -> key:string -> ?sc:string -> name:string ->
  ?timestamp:timestamp -> string -> unit Lwt.t

val insert_supercolumn : conn_pool -> ?level:level ->
  cf:string -> key:string -> name:string -> ?timestamp:timestamp ->
  (string * string) list -> unit Lwt.t

(** Use the timestamp in [column] unless another one is specified. *)
val insert_column : conn_pool -> ?level:level ->
  cf:string -> key:string -> ?sc:string -> ?timestamp:timestamp -> column -> unit Lwt.t

val remove_key : conn_pool -> ?level:level ->
  cf:string -> ?timestamp:timestamp -> string -> unit Lwt.t

val remove_column : conn_pool -> ?level:level ->
  cf:string -> key:string -> ?sc:string -> ?timestamp:timestamp -> string -> unit Lwt.t

val remove_supercolumn : conn_pool -> ?level:level ->
  cf:string -> key:string -> ?timestamp:timestamp -> string -> unit Lwt.t

(** (key * (column_family * mutation list) list) list *)
val batch_mutate : conn_pool -> ?level:level ->
  (string * (string * mutation list) list) list -> unit Lwt.t

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

  val get : conn_pool -> ?level:level -> 'a column -> key:string -> 'a option Lwt.t

  val set : conn_pool -> ?level:level ->
    'a column -> key:string -> ?timestamp:timestamp -> 'a -> unit Lwt.t

  val get' : conn_pool -> ?level:level ->
    sc:string -> 'a subcolumn -> key:string -> 'a option Lwt.t

  val set' : conn_pool -> ?level:level ->
    sc:string -> 'a subcolumn -> key:string -> ?timestamp:timestamp -> 'a ->
    unit Lwt.t
end
