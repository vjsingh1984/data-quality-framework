# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unity Catalog provider for Databricks."""
import logging

from dq.catalog.base import CatalogProvider

logger = logging.getLogger(__name__)


class UnityCatalogProvider(CatalogProvider):
    """Catalog provider for Databricks Unity Catalog.

    Unity Catalog uses three-level namespacing:
        ``<catalog>.<schema>.<table>``

    This provider resolves tables using Spark SQL with three-part names.
    The SparkSession must be configured with Unity Catalog enabled
    (standard on Databricks Runtime 11.3+).

    Configuration example::

        dqframework {
          catalog_type = "unity"
          dataframes {
            orders {
              catalog = "main"
              database = "sales"
              table = "orders"
            }
          }
        }

    Or using fully-qualified names directly::

        dqframework {
          dataframes {
            orders = "main.sales.orders"
          }
        }
    """

    def get_dataframe(self, table_reference, database=None, catalog=None):
        """Load a table as a Spark DataFrame via Unity Catalog.

        Supports three-part names (catalog.schema.table), two-part names
        (schema.table using the current catalog), or component-based
        resolution when database and catalog args are provided.

        Args:
            table_reference: Table name, schema.table, or catalog.schema.table.
            database: Optional schema name.
            catalog: Optional catalog name.

        Returns:
            Spark DataFrame.
        """
        full_name = self._resolve_table_name(table_reference, database, catalog)
        logger.info("Loading DataFrame from Unity Catalog: %s", full_name)
        return self._spark.table(full_name)

    def get_table_schema(self, table_reference, database=None, catalog=None):
        """Fetch table schema from Unity Catalog.

        Args:
            table_reference: Table name, schema.table, or catalog.schema.table.
            database: Optional schema name.
            catalog: Optional catalog name.

        Returns:
            pyspark.sql.types.StructType.
        """
        full_name = self._resolve_table_name(table_reference, database, catalog)
        logger.info("Fetching schema from Unity Catalog: %s", full_name)
        return self._spark.table(full_name).schema

    def table_exists(self, table_reference, database=None, catalog=None):
        """Check if a table exists in Unity Catalog.

        Args:
            table_reference: Table name, schema.table, or catalog.schema.table.
            database: Optional schema name.
            catalog: Optional catalog name.

        Returns:
            bool.
        """
        full_name = self._resolve_table_name(table_reference, database, catalog)
        try:
            self._spark.sql(f"DESCRIBE TABLE {full_name}")
            return True
        except Exception as e:
            logger.debug("Table %s not found in Unity Catalog: %s", full_name, e)
            return False

    def list_schemas(self, catalog_name=None):
        """List schemas in a Unity Catalog.

        Args:
            catalog_name: Catalog to list schemas from. Uses current catalog if None.

        Returns:
            List of schema names.
        """
        if catalog_name:
            rows = self._spark.sql(f"SHOW SCHEMAS IN {catalog_name}").collect()
        else:
            rows = self._spark.sql("SHOW SCHEMAS").collect()
        return [row[0] for row in rows]

    def list_tables(self, schema_name, catalog_name=None):
        """List tables in a Unity Catalog schema.

        Args:
            schema_name: Schema name to list tables from.
            catalog_name: Optional catalog name. Uses current catalog if None.

        Returns:
            List of table names.
        """
        full_schema = f"{catalog_name}.{schema_name}" if catalog_name else schema_name
        rows = self._spark.sql(f"SHOW TABLES IN {full_schema}").collect()
        return [row["tableName"] for row in rows]

    def set_current_catalog(self, catalog_name):
        """Set the current active catalog.

        Args:
            catalog_name: Name of the catalog to activate.
        """
        self._spark.sql(f"USE CATALOG {catalog_name}")
        logger.info("Switched to Unity Catalog: %s", catalog_name)

    def _resolve_table_name(self, table_reference, database=None, catalog=None):
        """Resolve a table reference to a fully-qualified three-part name.

        If table_reference already contains dots (catalog.schema.table or
        schema.table), it is used as-is. Otherwise, database and catalog
        args are prepended.

        Args:
            table_reference: Table name or qualified reference.
            database: Optional schema name.
            catalog: Optional catalog name.

        Returns:
            Fully-qualified table name string.
        """
        parts = table_reference.split(".")
        if len(parts) == 3:
            # Already fully qualified: catalog.schema.table
            return table_reference
        elif len(parts) == 2:
            # schema.table - prepend catalog if provided
            if catalog:
                return f"{catalog}.{table_reference}"
            return table_reference
        else:
            # Just table name - build from components
            return self._build_full_table_name(table_reference, database, catalog)
