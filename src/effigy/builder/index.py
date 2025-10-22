from sqlalchemy import Index, Table


class IndexConfiguration:
    def __init__(self, columns: list[str], unique: bool, *, name: str | None = None):
        self._columns = columns
        self._unique = unique
        self._name = name

    def create_index(self, table: Table, table_name: str) -> Index:
        index_cols = [table.c[col_name] for col_name in self._columns]

        if self._name:
            index_name = self._name
        else:
            prefix = "uq" if self._unique else "ix"
            col_names = "_".join(self._columns)
            index_name = f"{prefix}_{table_name}_{col_names}"
        return Index(index_name, *index_cols, unique=self._unique)
