
"""
Minimal REPL stub:
  - read SQL
  - call sql pipeline -> logical plan
  - run executor
"""
import sys, json
from ..sql.lexer import Lexer
from ..sql.parser import Parser
from ..sql.semantic import SemanticAnalyzer
from ..sql.planner import Planner

def main():
    print("mini_db REPL (stub). Type SQL; Ctrl+C to exit.")
    while True:
        try:
            sql = input("> ")
        except KeyboardInterrupt:
            print("\nBye."); break
        try:
            # tokens = list(Lexer(sql).tokens())
            # ast = Parser().parse(tokens)
            # sem = SemanticAnalyzer().analyze(ast)
            # plan = Planner().to_logical_plan(sem)
            # print(json.dumps(plan, indent=2))
            print("Pipeline not implemented yet.")
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()
