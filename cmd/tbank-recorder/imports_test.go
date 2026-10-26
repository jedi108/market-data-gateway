package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"strings"
	"testing"
)

// The recorder shares the T-Bank quota with the paper contour: only the
// MarketDataStream client and message types may be used from investapi.
func TestOnlyMarketDataStreamAPI(t *testing.T) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(i fs.FileInfo) bool { return !strings.HasSuffix(i.Name(), "_test.go") }, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range pkgs {
		for _, f := range p.Files {
			ast.Inspect(f, func(n ast.Node) bool {
				sel, ok := n.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				if x, ok := sel.X.(*ast.Ident); ok && x.Name == "investapi" {
					for _, bad := range []string{"Orders", "StopOrder", "Operations", "Sandbox", "UsersService"} {
						if strings.Contains(sel.Sel.Name, bad) {
							t.Errorf("%s: forbidden investapi.%s", fset.Position(sel.Pos()), sel.Sel.Name)
						}
					}
				}
				return true
			})
		}
	}
}
