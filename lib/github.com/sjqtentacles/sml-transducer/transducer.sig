(* transducer.sig

   Composable, fused transducers in pure Standard ML.

   A transducer transforms a *reducing function* into another reducing
   function, independent of the source or destination collection. Composing
   transducers therefore composes their per-element work into a single pass
   over the input, with no intermediate collections allocated.

   Clojure's transducers are rank-2 polymorphic — a transducer works for *any*
   accumulator type `r`. Standard ML has no rank-2 polymorphism, so we expose
   the accumulator `'r` as an explicit third type parameter of `xform`. A
   transducer value built by `comp` is therefore monomorphic in `'r` (the value
   restriction), so to drive the *same* pipeline at two different accumulator
   types, build it from a function (a thunk) so each use re-instantiates `'r`.

   Early termination (for `take`/`takeWhile`) is modelled with a `status`
   wrapper, `More`/`Stop`, threaded through the reducer; the driver stops
   pulling as soon as it sees `Stop`. Stateful transducers (`take`, `drop`,
   `dedupe`) keep their counter/last-seen in a `ref` that is created fresh each
   time the transducer is applied to a reducer, so every run is independent and
   deterministic. No FFI, threads, clock or randomness: identical results under
   MLton and Poly/ML. *)

signature TRANSDUCER =
sig
  datatype 'r status = More of 'r | Stop of 'r

  (* a transducer maps a downstream reducer (over 'b) to an upstream one (over 'a) *)
  type ('r, 'a, 'b) xform = ('r -> 'b -> 'r status) -> ('r -> 'a -> 'r status)

  (* ---- transducers ---- *)
  val map       : ('a -> 'b) -> ('r, 'a, 'b) xform
  val filter    : ('a -> bool) -> ('r, 'a, 'a) xform
  val remove    : ('a -> bool) -> ('r, 'a, 'a) xform      (* complement of filter *)
  val take      : int -> ('r, 'a, 'a) xform
  val drop      : int -> ('r, 'a, 'a) xform
  val takeWhile : ('a -> bool) -> ('r, 'a, 'a) xform
  val mapcat    : ('a -> 'b list) -> ('r, 'a, 'b) xform    (* map then flatten *)
  val cat       : ('r, 'a list, 'a) xform                  (* flatten a stream of lists *)
  val dedupe    : ('a * 'a -> bool) -> ('r, 'a, 'a) xform  (* drop consecutive dups *)

  (* ---- composition (left-to-right in data-flow order) ---- *)
  val identity  : ('r, 'a, 'a) xform
  val comp      : ('r, 'a, 'b) xform -> ('r, 'b, 'c) xform -> ('r, 'a, 'c) xform
  val compList  : ('r, 'a, 'a) xform list -> ('r, 'a, 'a) xform

  (* ---- runners over lists ---- *)
  (* transduce xf rf init coll: transform, then reduce with a plain reducer *)
  val transduce : ('r, 'a, 'b) xform -> ('r -> 'b -> 'r) -> 'r -> 'a list -> 'r
  (* into xf coll: transform, collecting outputs into a list (order preserved) *)
  val into      : ('b list, 'a, 'b) xform -> 'a list -> 'b list
  (* xreduce: the low-level fused fold of a status-aware reducer (early stop) *)
  val xreduce   : ('r -> 'a -> 'r status) -> 'r -> 'a list -> 'r
end
