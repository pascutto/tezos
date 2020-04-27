exception Copy_error of string

module IO = Layers_IO.Unix

module type LAYERED_S = sig
  include Pack.S

  module U : Pack.S

  module L : Pack.S

  type 'a upper = 'a U.t

  val v :
    'a upper ->
    'a upper ->
    ?fresh:bool ->
    ?readonly:bool ->
    ?lru_size:int ->
    index:index ->
    string ->
    Lwt_mutex.t ->
    bool ->
    IO.t ->
    'a t Lwt.t

  val batch : unit -> 'a Lwt.t

  val unsafe_append : 'a t -> key -> value -> unit Lwt.t

  val project :
    'a t ->
    [ `Read | `Write ] upper ->
    [ `Read | `Write ] upper ->
    [ `Read | `Write ] t

  val layer_id : [ `Read ] t -> key -> int Lwt.t

  type 'a layer_type =
    | Upper : [ `Read | `Write ] U.t layer_type
    | Lower : [ `Read | `Write ] L.t layer_type

  val already_in_dst : 'l layer_type * 'l -> key -> bool Lwt.t

  val check_and_copy :
    'l layer_type * 'l ->
    [ `Read ] t ->
    aux:(value -> unit Lwt.t) ->
    string ->
    key ->
    unit Lwt.t

  val batch_lower : 'a t -> ([ `Read | `Write ] L.t -> 'b Lwt.t) -> 'b Lwt.t

  val current_upper : 'a t -> 'a U.t

  val previous_upper : 'a t -> 'a U.t

  val lower : 'a t -> [ `Read ] L.t

  val flip_upper : 'a t -> unit
end

module Content_addressable
    (H : Irmin.Hash.S)
    (Index : Pack_index.S)
    (S : Pack.S with type index = Index.t and type key = H.t) :
  LAYERED_S
    with type key = S.key
     and type value = S.value
     and type index = S.index
     and module U = S
     and type L.key = S.key
     and type L.value = S.value

module type AW = sig
  include Irmin.ATOMIC_WRITE_STORE

  val v : ?fresh:bool -> ?readonly:bool -> string -> t Lwt.t

  val sync : t -> unit
end

module Atomic_write (K : Irmin.Branch.S) (A : AW with type key = K.t) : sig
  include AW with type key = A.key and type value = A.value

  val v :
    A.t ->
    A.t ->
    ?fresh:bool ->
    ?readonly:bool ->
    string ->
    Lwt_mutex.t ->
    bool ->
    IO.t ->
    t Lwt.t

  val copy :
    t ->
    mem_commit_lower:(value -> bool Lwt.t) ->
    mem_commit_upper:(value -> bool Lwt.t) ->
    unit Lwt.t

  val flip_upper : t -> unit

  val sync : t -> unit
end

module type LAYERED_MAKER = sig
  type key

  type index

  (** Save multiple kind of values in the same pack file. Values will be
      distinguished using [V.magic], so they have to all be different. *)
  module Make (V : Pack.ELT with type hash := key) :
    LAYERED_S
      with type key = key
       and type value = V.t
       and type index = index
       and type U.index = index
       and type U.key = key
       and type L.key = key
       and type U.value = V.t
       and type L.value = V.t
end

module Pack_Maker
    (H : Irmin.Hash.S)
    (Index : Pack_index.S)
    (S : Pack.MAKER with type key = H.t and type index = Index.t) :
  LAYERED_MAKER with type key = S.key and type index = S.index
