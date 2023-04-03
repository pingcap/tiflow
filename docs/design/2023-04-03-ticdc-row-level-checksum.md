# TiCDC support row level checksum calculation and comparison

- Author(s): [3AceShowHand](https://github.com/3AceShowHand)
- Tracking Issue: https://github.com/pingcap/tiflow/issues/8718

- [Introduction](#introduction)
- [Motivation or Background](#motivation-or-background)
- [Detailed Design](#detailed-design)
- [Test Design](#test-design)
- [Impacts & Risks](#impacts--risks)
- [Investigation & Alternatives](#investigation--alternatives)
- [Unresolved Questions](#unresolved-questions)

## Introduction

This document describe how the TiCDC support row level checksum mechanism, it's helpful to detect whether the row changed event is corrupted during the data transfer process.

## Motivation or Background

Data integrity is especially important for data synchronization systems, but TiCDC does not support end-to-end data integrity verification yet.

Each row changed event should keep unchanged during the whole synchronization progress as the basic to guarantee the correctness of the whole synchronization task.

## Detailed Design

N/A

## Test Design

N/A

## Impacts & Risks

N/A

## Investigation & Alternatives

N/A

## Unresolved Questions

N/A