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

  (* A reducer bundles a stepping function with a completion (flush) hook.
     `step acc x` folds one element; `complete acc` is called once after the
     last element so buffering stages (partitionAll, distinct) can emit any
     trailing state. Most stages forward `complete` unchanged. *)
  type ('r, 'b) reducer = { step : 'r -> 'b -> 'r status, complete : 'r -> 'r }

  (* a transducer maps a downstream reducer (over 'b) to an upstream one (over 'a) *)
  type ('r, 'a, 'b) xform = ('r, 'b) reducer -> ('r, 'a) reducer

  (* Build a reducer from a plain step (identity completion). *)
  val reducer    : ('r -> 'b -> 'r status) -> ('r, 'b) reducer
  (* Build a reducer with an explicit completion hook. *)
  val completing : ('r -> 'b -> 'r status) -> ('r -> 'r) -> ('r, 'b) reducer

  (* ---- transducers ---- *)
  val map        : ('a -> 'b) -> ('r, 'a, 'b) xform
  val mapIndexed : (int -> 'a -> 'b) -> ('r, 'a, 'b) xform
  val filter     : ('a -> bool) -> ('r, 'a, 'a) xform
  val remove     : ('a -> bool) -> ('r, 'a, 'a) xform      (* complement of filter *)
  val keep       : ('a -> 'b option) -> ('r, 'a, 'b) xform (* keep SOME results *)
  val keepIndexed : (int -> 'a -> 'b option) -> ('r, 'a, 'b) xform
  val take       : int -> ('r, 'a, 'a) xform
  val takeNth    : int -> ('r, 'a, 'a) xform               (* every nth element (1-based) *)
  val drop       : int -> ('r, 'a, 'a) xform
  val takeWhile  : ('a -> bool) -> ('r, 'a, 'a) xform
  val dropWhile  : ('a -> bool) -> ('r, 'a, 'a) xform
  val interpose  : 'a -> ('r, 'a, 'a) xform                (* insert sep between elements *)
  val mapcat     : ('a -> 'b list) -> ('r, 'a, 'b) xform    (* map then flatten *)
  val cat        : ('r, 'a list, 'a) xform                  (* flatten a stream of lists *)
  val dedupe     : ('a * 'a -> bool) -> ('r, 'a, 'a) xform  (* drop consecutive dups *)
  val distinct   : ('a * 'a -> bool) -> ('r, 'a, 'a) xform  (* drop ALL later dups *)
  (* partitionAll n: group into lists of length n; the flush hook emits the
     final short partition. *)
  val partitionAll : int -> ('r, 'a, 'a list) xform

  (* ---- composition (left-to-right in data-flow order) ---- *)
  val identity  : ('r, 'a, 'a) xform
  val comp      : ('r, 'a, 'b) xform -> ('r, 'b, 'c) xform -> ('r, 'a, 'c) xform
  val compList  : ('r, 'a, 'a) xform list -> ('r, 'a, 'a) xform

  (* ---- runners ---- *)
  (* transduce xf rf init coll: transform, then reduce with a plain reducer;
     the completion hook runs after the last element. *)
  val transduce : ('r, 'a, 'b) xform -> ('r -> 'b -> 'r) -> 'r -> 'a list -> 'r
  (* into xf coll: transform, collecting outputs into a list (order preserved) *)
  val into      : ('b list, 'a, 'b) xform -> 'a list -> 'b list
  (* intoString xf coll: collect string outputs concatenated in order *)
  val intoString : (string, 'a, string) xform -> 'a list -> string
  (* intoArray xf coll: collect outputs into a vector (order preserved) *)
  val intoArray  : ('b list, 'a, 'b) xform -> 'a list -> 'b vector
  (* xreduce: the low-level fused fold of a reducer (early stop + completion) *)
  val xreduce   : ('r, 'a) reducer -> 'r -> 'a list -> 'r
end
