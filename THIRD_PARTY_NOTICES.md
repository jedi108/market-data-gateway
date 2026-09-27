# Third-party notices

## TBank Invest API generated bindings

The Go files under `internal/provider/tbank/gen/investapi/` are generated from
protocol buffer definitions of the TBank Invest API.

- Upstream source: <https://github.com/RussianInvestments/invest-python>, pinned revision `2a0074a`
  (`common.proto`, `marketdata.proto`, `field_behavior.proto`).
- Generated with `protoc v5.29.3`, `protoc-gen-go v1.36.12`, `protoc-gen-go-grpc v1.5.1`.

The upstream definitions are licensed under the Apache License, Version 2.0:

```text
                                 Apache License
                           Version 2.0, January 2004
                        http://www.apache.org/licenses/

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
```

## Go module dependencies

Go module dependencies and their licenses are declared in `go.mod`/`go.sum`.
Notable runtime dependencies: `google.golang.org/grpc`, `google.golang.org/protobuf`,
`gopkg.in/yaml.v3`, and the pure-Go SQLite driver `modernc.org/sqlite`.
