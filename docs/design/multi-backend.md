# Multi-backend

Concept: a backend that's a `Vec<Box<dyn Backend>>` or whatever.

## IDs?

Change or commit ID: `[<byte>|<max id length>]`?
Or actually Lamport clock it?
How else can we figure out the backend from the commit?

Perhaps some auto-incrementing ID that maps to the Lambert clock?
Otherwise we'd have to concatenate the change IDs.
What are we allowed to write under the `.jj` dir for this?

Why is only the root change ID important?

What is a file ID? Is it ok to have collisions if the paths can disambiguate?

### Merges?!

- A commit can have multiple parents? Are merges going to be binary? Or is there some other rule?

Note to self: the git backend uses interior mutability since the TableStore has it.

### Bookmarks?

I guess these aren't actually up to the backend?

## CLI

Where is the binding? What would actually have to happen to make this work?

Should there be some way to do a per-backend thing?

How to initialize stuff? Is "automatic" enough?

## Module registry

- Store what and where all the backends are.
- How to auto-detect the new module and the fact that it's not merely a new directory?

## Programming notes

Why not use const generics for ID lengths?
