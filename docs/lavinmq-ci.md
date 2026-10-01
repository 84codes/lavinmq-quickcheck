# Running the suite in LavinMQ's CI

Each `v*` tag of this repo publishes a static (musl) executable that holds
the whole test suite: `amqp-quickcheck-tests-x86_64-linux`, plus a
`.sha256`. It needs no Rust toolchain or system libraries, only a LavinMQ
on `localhost:5672` with the management API on `localhost:15672` and
`guest:guest`.

## Example jobs

These fit LavinMQ's `ci.yml`, whose `compile` job uploads `bin/` as an
artifact. Replace `OWNER` with this repo's GitHub owner, and pin `QC_VERSION`
so a new test release can't turn LavinMQ's CI red on its own.

```yaml
  amqp-quickcheck:
    name: amqp-quickcheck property tests
    runs-on: ubuntu-24.04
    needs: compile
    env:
      QC_VERSION: v0.1.0
      QC_BIN: amqp-quickcheck-tests-x86_64-linux
    steps:
      - name: Install LavinMQ dependencies
        run: |
          sudo apt-get update
          sudo apt-get install -y liblz4-1

      - uses: actions/download-artifact@v8
        with:
          name: bin
          path: bin

      - name: Run LavinMQ in background
        run: |
          chmod +x bin/*
          bin/lavinmq --data-dir=/tmp/amqp --bind=:: &

      - name: Download amqp-quickcheck
        env:
          GH_TOKEN: ${{ github.token }}
        run: |
          gh release download "$QC_VERSION" -R OWNER/amqp-quickcheck -p "$QC_BIN*"
          sha256sum -c "$QC_BIN.sha256"
          chmod +x "$QC_BIN"

      - name: Wait for LavinMQ
        run: |
          for _ in $(seq 60); do
            curl -sf -u guest:guest http://localhost:15672/api/overview >/dev/null && exit 0
            sleep 1
          done
          exit 1

      - name: Run property tests
        # Known flaky under parallel load; see AGENTS.md (Gotchas).
        run: ./"$QC_BIN" --skip odd_x_delay_delivers_immediately

  amqp-quickcheck-known-bugs:
    name: amqp-quickcheck known LavinMQ bugs
    runs-on: ubuntu-24.04
    needs: compile
    # These assert the *fixed* behaviour of open LavinMQ bugs. A pass means
    # a bug is fixed and its #[ignore] can be removed here.
    continue-on-error: true
    env:
      QC_VERSION: v0.1.0
      QC_BIN: amqp-quickcheck-tests-x86_64-linux
    steps:
      # … same setup steps as above …
      - name: Run ignored bug probes
        run: ./"$QC_BIN" --ignored
```

## Tuning

The binary accepts the usual libtest flags: test-name filters, `--skip`,
`--ignored`, `--test-threads=N`, `--list`. quickcheck reads
`QUICKCHECK_TESTS` (cases per property, default 100). The full suite at
the default takes about 45 s. quickcheck 1.0 has no seed option, so keep
the job logs: a failure prints the generated input that caused it.

Each run leaves `qc-*` vhosts behind, which doesn't matter on a
throwaway CI broker.
