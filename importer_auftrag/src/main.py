import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from src.auftrag import import_auftrag, import_auftrag_position

def main():
    try:
        import_auftrag()
        import_auftrag_position()
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
