"""
Initialize the SQLite/Postgres warehouse by creating SQLModel tables.
Run:
  poetry run python scripts/init_db.py
"""

from de_challenge.data_access.db import create_all


def main() -> None:
    create_all()
    print("Warehouse tables created.")


if __name__ == "__main__":
    main()
