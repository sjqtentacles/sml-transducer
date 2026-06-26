# sml-transducer

[![CI](https://github.com/sjqtentacles/sml-transducer/actions/workflows/ci.yml/badge.svg)](https://github.com/sjqtentacles/sml-transducer/actions/workflows/ci.yml)

Composable, **fused transducers** in pure Standard ML — `map`, `filter`,
`take`, `drop`, `mapcat`, `dedupe`, `distinct`, `partitionAll`, `interpose`, …
that compose into a single pass over the input with no intermediate
collections, plus early termination **and** an end-of-stream flush hook.

No dependencies, no FFI, no threads, no clock, no randomness: the same inputs
always produce the same outputs under **MLton** and **Poly/ML**.

> **Breaking change.** Reducers are now a `{step, complete}` record rather than
> a bare stepping function, so transducers can flush buffered state after the
> last element (needed by `partitionAll`). The `xform` type and `xreduce` now
> take a `reducer` record. If you previously called `xreduce` with a raw
> stepping function, wrap it with `reducer`; pipelines built from `map`/`filter`/
> `comp`/`into`/`transduce` are unaffected at the call site.

## What is a transducer?

A transducer transforms a **reducing function** (`'r -> 'b -> 'r`) into another
reducing function, independent of the source or destination collection.
Composing transducers composes their per-element work, so

```sml
into (comp (map inc) (comp (filter even) (take 3))) [1..100]
```

does its mapping, filtering and taking in **one pass**, allocating no
intermediate lists — and `take 3` stops the traversal as soon as the third
element is produced (the demo's step counter shows only 5 source elements are
ever touched, not 100).

## Rank-2 polymorphism and the `'r` parameter

Clojure transducers are rank-2 polymorphic: one transducer works for *any*
accumulator type. Standard ML has no rank-2 polymorphism, so `xform` exposes
the accumulator `'r` as an explicit third type parameter:

```sml
datatype 'r status = More of 'r | Stop of 'r
type ('r,'b) reducer = { step : 'r -> 'b -> 'r status, complete : 'r -> 'r }
type ('r,'a,'b) xform = ('r,'b) reducer -> ('r,'a) reducer
```

The `complete` hook runs once after the last element so buffering stages can
emit any trailing state; most stages forward it unchanged.

A pipeline built by `comp` is therefore monomorphic in `'r` (the value
restriction). To drive the *same* pipeline at two different accumulator types,
build it from a thunk so each use re-instantiates `'r`:

```sml
fun pipe () = comp (map inc) (filter even)
val xs  = into (pipe ()) [1,2,3]        (* 'r = int list *)
val n   = transduce (pipe ()) (fn a => fn b => a + b) 0 [1,2,3]   (* 'r = int *)
```

Early termination is carried by the `status` wrapper (`More`/`Stop`); the
driver stops pulling on `Stop` and then runs the pipeline's `complete` hook.
Stateful stages (`take`, `drop`, `dedupe`, `distinct`, `partitionAll`) keep
their counter/buffer in a `ref` allocated **fresh each run** (when the
transducer is applied to a reducer), so runs are independent and deterministic.

## API

```sml
structure Transducer : sig
  datatype 'r status = More of 'r | Stop of 'r
  type ('r,'b) reducer = { step : 'r -> 'b -> 'r status, complete : 'r -> 'r }
  type ('r,'a,'b) xform = ('r,'b) reducer -> ('r,'a) reducer

  val reducer    : ('r -> 'b -> 'r status) -> ('r,'b) reducer
  val completing : ('r -> 'b -> 'r status) -> ('r -> 'r) -> ('r,'b) reducer

  val map        : ('a -> 'b) -> ('r,'a,'b) xform
  val mapIndexed : (int -> 'a -> 'b) -> ('r,'a,'b) xform
  val filter     : ('a -> bool) -> ('r,'a,'a) xform
  val remove     : ('a -> bool) -> ('r,'a,'a) xform
  val keep       : ('a -> 'b option) -> ('r,'a,'b) xform
  val keepIndexed : (int -> 'a -> 'b option) -> ('r,'a,'b) xform
  val take       : int -> ('r,'a,'a) xform
  val takeNth    : int -> ('r,'a,'a) xform
  val drop       : int -> ('r,'a,'a) xform
  val takeWhile  : ('a -> bool) -> ('r,'a,'a) xform
  val dropWhile  : ('a -> bool) -> ('r,'a,'a) xform
  val interpose  : 'a -> ('r,'a,'a) xform
  val mapcat     : ('a -> 'b list) -> ('r,'a,'b) xform
  val cat        : ('r, 'a list, 'a) xform
  val dedupe     : ('a * 'a -> bool) -> ('r,'a,'a) xform
  val distinct   : ('a * 'a -> bool) -> ('r,'a,'a) xform
  val partitionAll : int -> ('r, 'a, 'a list) xform

  val identity  : ('r,'a,'a) xform
  val comp      : ('r,'a,'b) xform -> ('r,'b,'c) xform -> ('r,'a,'c) xform
  val compList  : ('r,'a,'a) xform list -> ('r,'a,'a) xform

  val transduce  : ('r,'a,'b) xform -> ('r -> 'b -> 'r) -> 'r -> 'a list -> 'r
  val into       : ('b list, 'a, 'b) xform -> 'a list -> 'b list
  val intoString : (string, 'a, string) xform -> 'a list -> string
  val intoArray  : ('b list, 'a, 'b) xform -> 'a list -> 'b vector
  val xreduce    : ('r,'a) reducer -> 'r -> 'a list -> 'r
end
```

- `keep`/`keepIndexed` keep only the `SOME` results of a function (filter+map in
  one). `distinct` drops *all* later duplicates (unlike `dedupe`, which only
  collapses *consecutive* runs). `takeNth n` keeps every nth element (1-based).
  `interpose sep` inserts `sep` between emitted elements. `partitionAll n` groups
  into length-`n` lists, using the flush hook to emit the final short group.
- `intoString` concatenates string outputs; `intoArray` collects into a vector.

## Example

```sml
open Transducer
fun isEven n = n mod 2 = 0
fun add a b = a + b

(* collect with `into` *)
val [2,4,6]   = into (comp (map (fn x => x + 1)) (filter isEven)) [1,2,3,4,5,6]
val [1,1,2,2] = into (mapcat (fn x => [x, x])) [1, 2]
val [1,2,3,1] = into (dedupe (op =)) [1,1,2,2,2,3,1,1]
val [1,2,3,4] = into (distinct (op =)) [1,2,1,3,2,4,1]    (* all later dups dropped *)
val [[1,2],[3,4],[5]] = into (partitionAll 2) [1,2,3,4,5] (* flush emits trailing [5] *)
val "A-B-C"   = intoString (interpose "-") ["A","B","C"]

(* reduce with `transduce` *)
val 30 = transduce (filter isEven) add 0 [1,2,3,4,5,6,7,8,9,10]
val 6  = transduce (take 3) add 0 [1,2,3,4,5,6,7,8,9,10]   (* early stop *)
```

Running [`examples/demo.sml`](examples/demo.sml) with `make example` prints:

```
Fused pipeline over [1..100]:  map (+1) |> filter even |> take 3
  result            = [2,4,6]
  source elements processed = 5 (not 100 - single pass, early stop)

mapcat / cat / dedupe:
  mapcat (fn x=>[x,x]) [1,2,3] = [1,1,2,2,3,3]
  cat [[1,2],[3],[4,5]]        = [1,2,3,4,5]
  dedupe [1,1,2,2,2,3,1,1]     = [1,2,3,1]

Reducing runners:
  transduce (filter even) (+) over [1..10] = 30
  transduce (take 3) (+) over [1..100]     = 6 (early stop)
```

## Build & test

Requires [MLton](http://mlton.org/) and/or [Poly/ML](https://polyml.org/).

```sh
make test        # build + run the suite under MLton
make test-poly   # run the suite under Poly/ML
make all-tests   # both
make example     # build + run the demo
make clean
```

## Installing with smlpkg

```sh
smlpkg add github.com/sjqtentacles/sml-transducer
smlpkg sync
```

Reference `lib/github.com/sjqtentacles/sml-transducer/transducer.mlb` from your
own `.mlb` (MLton / MLKit), or feed `sources.mlb` to `tools/polybuild`
(Poly/ML).

## Layout

```
sml.pkg                                          smlpkg manifest
Makefile                                         MLton + Poly/ML targets
.github/workflows/ci.yml                         CI: MLton + Poly/ML
lib/github.com/sjqtentacles/sml-transducer/
  transducer.sig   TRANSDUCER signature
  transducer.sml   reducer transformers + runners
  sources.mlb      ordered source list
  transducer.mlb   public basis
examples/
  demo.sml         fusion + early-stop walkthrough
test/
  harness.sml      shared assertion harness
  test.sml         per-stage + composition + fusion + runner vectors (58 checks)
  entry.sml / main.sml
tools/polybuild    Poly/ML build wrapper
```

## Tests

58 deterministic checks: every stage (`map`/`mapIndexed`/`filter`/`remove`/
`keep`/`keepIndexed`/`take`/`takeNth`/`drop`/`takeWhile`/`dropWhile`/`interpose`/
`mapcat`/`cat`/`dedupe`/`distinct`/`partitionAll`) at its edge cases; composition
order (`comp`, `compList`, `identity`); the fused `map ∘ filter ∘ take` pipeline
proven equal to a staged `List.*` pipeline; and — via a step counter — that the
fusion is a single pass and `take`/`takeWhile` terminate early (5 source
elements touched, not 100). The runners `transduce`/`into`/`intoString`/
`intoArray`/`xreduce` are exercised including early-stopping reductions and the
`partitionAll` flush hook. Run `make all-tests` to verify identical output under
both compilers.

## License

MIT. See [LICENSE](LICENSE).
