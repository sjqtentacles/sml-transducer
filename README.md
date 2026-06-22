# sml-transducer

[![CI](https://github.com/sjqtentacles/sml-transducer/actions/workflows/ci.yml/badge.svg)](https://github.com/sjqtentacles/sml-transducer/actions/workflows/ci.yml)

Composable, **fused transducers** in pure Standard ML — `map`, `filter`,
`take`, `drop`, `mapcat`, `dedupe`, … that compose into a single pass over the
input with no intermediate collections, plus early termination.

No dependencies, no FFI, no threads, no clock, no randomness: the same inputs
always produce the same outputs under **MLton** and **Poly/ML**.

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
type ('r,'a,'b) xform = ('r -> 'b -> 'r status) -> ('r -> 'a -> 'r status)
```

A pipeline built by `comp` is therefore monomorphic in `'r` (the value
restriction). To drive the *same* pipeline at two different accumulator types,
build it from a thunk so each use re-instantiates `'r`:

```sml
fun pipe () = comp (map inc) (filter even)
val xs  = into (pipe ()) [1,2,3]        (* 'r = int list *)
val n   = transduce (pipe ()) (fn a => fn b => a + b) 0 [1,2,3]   (* 'r = int *)
```

Early termination is carried by the `status` wrapper (`More`/`Stop`); the
driver stops pulling on `Stop`. Stateful stages (`take`, `drop`, `dedupe`) keep
their counter/last-seen in a `ref` allocated **fresh each run** (when the
transducer is applied to a reducer), so runs are independent and deterministic.

## API

```sml
structure Transducer : sig
  datatype 'r status = More of 'r | Stop of 'r
  type ('r,'a,'b) xform = ('r -> 'b -> 'r status) -> ('r -> 'a -> 'r status)

  val map       : ('a -> 'b) -> ('r,'a,'b) xform
  val filter    : ('a -> bool) -> ('r,'a,'a) xform
  val remove    : ('a -> bool) -> ('r,'a,'a) xform
  val take      : int -> ('r,'a,'a) xform
  val drop      : int -> ('r,'a,'a) xform
  val takeWhile : ('a -> bool) -> ('r,'a,'a) xform
  val mapcat    : ('a -> 'b list) -> ('r,'a,'b) xform
  val cat       : ('r, 'a list, 'a) xform
  val dedupe    : ('a * 'a -> bool) -> ('r,'a,'a) xform

  val identity  : ('r,'a,'a) xform
  val comp      : ('r,'a,'b) xform -> ('r,'b,'c) xform -> ('r,'a,'c) xform
  val compList  : ('r,'a,'a) xform list -> ('r,'a,'a) xform

  val transduce : ('r,'a,'b) xform -> ('r -> 'b -> 'r) -> 'r -> 'a list -> 'r
  val into      : ('b list, 'a, 'b) xform -> 'a list -> 'b list
  val xreduce   : ('r -> 'a -> 'r status) -> 'r -> 'a list -> 'r
end
```

## Example

```sml
open Transducer
fun isEven n = n mod 2 = 0
fun add a b = a + b

(* collect with `into` *)
val [2,4,6]   = into (comp (map (fn x => x + 1)) (filter isEven)) [1,2,3,4,5,6]
val [1,1,2,2] = into (mapcat (fn x => [x, x])) [1, 2]
val [1,2,3,1] = into (dedupe (op =)) [1,1,2,2,2,3,1,1]

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
  test.sml         per-stage + composition + fusion + runner vectors (37 checks)
  entry.sml / main.sml
tools/polybuild    Poly/ML build wrapper
```

## Tests

37 deterministic checks: every stage (`map`/`filter`/`remove`/`take`/`drop`/
`takeWhile`/`mapcat`/`cat`/`dedupe`) at its edge cases; composition order
(`comp`, `compList`, `identity`); the fused `map ∘ filter ∘ take` pipeline
proven equal to a staged `List.*` pipeline; and — via a step counter — that the
fusion is a single pass and `take`/`takeWhile` terminate early (5 source
elements touched, not 100). The runners `transduce`/`into`/`xreduce` are
exercised including early-stopping reductions. Run `make all-tests` to verify
identical output under both compilers.

## License

MIT. See [LICENSE](LICENSE).
