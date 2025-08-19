from __future__ import annotations

from vector_search.indexer import index_sales


def main() -> None:
    stats = index_sales()
    print(f"Indexed {stats['indexed']} / {stats['total']} documents into Typesense.")


if __name__ == "__main__":
    main()
