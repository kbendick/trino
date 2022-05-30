/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.iceberg;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.trino.spi.QueryId;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testng.services.Flaky;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static io.trino.plugin.iceberg.IcebergMetadata.TRINO_QUERY_ID_NAME;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.iceberg.IcebergQueryRunner.createIcebergQueryRunner;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergSnapshots
        extends AbstractTestQueryFramework
{
    private static final Logger log = LoggerFactory.getLogger(TestIcebergSnapshots.class);
    private static final int ID_FIELD = 0;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createIcebergQueryRunner();
    }

    @Test
    public void testSnapshotSummariesHaveTrinoQueryId()
    {
        final String tableName = "test_snapshot_query_ids" + randomTableSuffix();

        try {
            List<QueryId> queryIds = Lists.newArrayList();
            // Create table.
            QueryId createTableQueryId = getDistributedQueryRunner()
                    .executeWithQueryId(getSession(), "CREATE TABLE " + tableName + " (a  bigint, b bigint)")
                    .getQueryId();

            logFormatted("QueryID for %s is %s", "Create Table", createTableQueryId);
            queryIds.add(createTableQueryId);

//            assertThat(getQueryIdsFromSnapshotsByCreationOrder(tableName))
//                    .containsExactly(createTableQueryId);

            // Insert
            QueryId appendQueryId = getDistributedQueryRunner()
                    .executeWithQueryId(getSession(), "INSERT INTO " + tableName + " VALUES(1, 1)")
                    .getQueryId();
            queryIds.add(appendQueryId);

            logFormatted("QueryID for %s is %s", "Single row append", appendQueryId);

            // Multi-value insert
            QueryId multiValueInsert = getDistributedQueryRunner()
                    .executeWithQueryId(getSession(), "INSERT INTO " + tableName + " VALUES (1, 2), (2, 1), (2, 2)")
                    .getQueryId();
            queryIds.add(multiValueInsert);

            logFormatted("QueryID for %s is %s", "multi-value insert", multiValueInsert);

            // Upgrade to Format v2
            QueryId alterTablePropertiesToFormatV2 = getDistributedQueryRunner()
                    .executeWithQueryId(getSession(), "ALTER TABLE " + tableName + " SET PROPERTIES format_version = 2")
                    .getQueryId();
            // TODO - This needs to be made into a commit that updates the snapshot.
//            queryIds.add(alterTablePropertiesToFormatV2);

            logFormatted("QueryID for %s is %s", "alter table to format v2", alterTablePropertiesToFormatV2);

            // Row level delete
            // TODO - This is generating multiple snapshots (as observed by snapshot summary query IDs)
            //  when it's only a = 1. The snapshots all have the same query ID, indicating that they all occurred
            //  in a transaction - Should transaction.apply() be used in some places (over .commit multiple times)?
            // It seems that each individual MOR delta file generates its own snapshot - is this intended?
            QueryId singleRowDeleteQueryId = getDistributedQueryRunner()
                    .executeWithQueryId(getSession(), "DELETE FROM " + tableName + " WHERE a = 2 AND b = 1")
                    .getQueryId();
            queryIds.add(singleRowDeleteQueryId);

            logFormatted("QueryID for %s is %s", "single row non-whole-file delete", singleRowDeleteQueryId);

            assertThat(getQueryIdsFromSnapshotsByCreationOrder(tableName))
                    .containsExactlyElementsOf(queryIds);

            // Ensure all Query IDs are unique.
            assertThat(Sets.newHashSet(queryIds))
                    .hasSameSizeAs(queryIds);

            List<MaterializedRow> summaries = getSnapshotSummariesByCreationOrder(tableName);
            log.error("ALL SNAPSHOT SUMMARIES ARE {}", summaries);

            log.error("THERE ARE {} total summaries", summaries.size());
        }
        finally {
            assertUpdate(format("DROP TABLE %s", tableName));
        }
    }

    @Flaky(issue = "hoo", match = ICEBERG_CATALOG)
    @Test
    public void testOptimizeSnapshotSummaryHasTrinoQueryId()
    {
        System.setProperty("io.trino.testng.services.FlakyTestRetryAnalyzer.enabled", "true");
        String tableName = "test_optimize_unpartitioned" + randomTableSuffix();
        assertUpdate(format("CREATE TABLE %s (a bigint, b bigint)", tableName));
        assertUpdate(format("ALTER TABLE %s SET PROPERTIES format_version = 2", tableName));
        assertUpdate(format("INSERT INTO %s VALUES(1, 1)", tableName), 1);
        assertUpdate(format("INSERT INTO %s VALUES(2, 2)", tableName), 1);

        QueryId optimizeQueryId = getDistributedQueryRunner()
                .executeWithQueryId(getSession(), "ALTER TABLE " + tableName + " EXECUTE optimize")
                .getQueryId();

        log.error("The optimize query id is " + optimizeQueryId);

        assertThat(getQueryIdsFromSnapshotsByCreationOrder(tableName))
                .contains(optimizeQueryId);
    }

    @Test
    public void testUpgradingToFormatVersionTwice()
    {
        String tableName = "test_multiple_format_upgrades" + randomTableSuffix();
        assertUpdate(format("CREATE TABLE %s (a bigint, b bigint)", tableName));
        assertUpdate(format("ALTER TABLE %s SET PROPERTIES format_version = 2", tableName));
        assertUpdate(format("INSERT INTO %s VALUES(1, 1)", tableName), 1);

        assertUpdate(format("ALTER TABLE %s set PROPERTIES format_version = 2", tableName));
    }

    @Test
    public void testReadingFromSpecificSnapshot()
    {
        String tableName = "test_reading_snapshot" + randomTableSuffix();
        assertUpdate(format("CREATE TABLE %s (a bigint, b bigint)", tableName));
        assertUpdate(format("INSERT INTO %s VALUES(1, 1)", tableName), 1);
        List<Long> ids = getSnapshotsIdsByCreationOrder(tableName);

        assertQuery(format("SELECT count(*) FROM \"%s@%d\"", tableName, ids.get(0)), "VALUES(0)");
        assertQuery(format("SELECT * FROM \"%s@%d\"", tableName, ids.get(1)), "VALUES(1,1)");
        assertUpdate(format("DROP TABLE %s", tableName));
    }

    @Test
    public void testSelectWithMoreThanOneSnapshotOfTheSameTable()
    {
        String tableName = "test_reading_snapshot" + randomTableSuffix();
        assertUpdate(format("CREATE TABLE %s (a bigint, b bigint)", tableName));
        assertUpdate(format("INSERT INTO %s VALUES(1, 1)", tableName), 1);
        assertUpdate(format("INSERT INTO %s VALUES(2, 2)", tableName), 1);
        assertUpdate(format("INSERT INTO %s VALUES(3, 3)", tableName), 1);
        List<Long> ids = getSnapshotsIdsByCreationOrder(tableName);

        assertQuery(format("SELECT * FROM %s", tableName), "SELECT * FROM (VALUES(1,1), (2,2), (3,3))");
        assertQuery(
                format("SELECT * FROM %1$s EXCEPT (SELECT * FROM \"%1$s@%2$d\" EXCEPT SELECT * FROM \"%1$s@%3$d\")", tableName, ids.get(2), ids.get(1)),
                "SELECT * FROM (VALUES(1,1), (3,3))");
        assertUpdate(format("DROP TABLE %s", tableName));
    }

    private List<Long> getSnapshotsIdsByCreationOrder(String tableName)
    {
        return getQueryRunner().execute(
                format("SELECT snapshot_id FROM \"%s$snapshots\" ORDER BY committed_at", tableName))
                .getMaterializedRows().stream()
                .map(row -> (Long) row.getField(ID_FIELD))
                .collect(toList());
    }

    private List<MaterializedRow> getSnapshotSummariesByCreationOrder(String tableName)
    {
        return new ArrayList<>(getQueryRunner().execute(
                        format("SELECT CAST(SUMMARY as JSON) FROM \"%s$snapshots\"", tableName))
                .getMaterializedRows());
    }

    private Stream<QueryId> getQueryIdsFromSnapshotsByCreationOrder(String tableName)
    {
        return getQueryRunner().execute(
                format("SELECT json_extract_scalar(CAST(SUMMARY AS JSON), '$.%s') FROM \"%s$snapshots\"", TRINO_QUERY_ID_NAME, tableName))
                .getOnlyColumn()
                .map(column -> QueryId.valueOf((String) column));
    }

    private void logFormatted(String formatStr, Object... args)
    {
        String formatted = format(formatStr, args);
        log.error("\tRAUL - " + formatted);
    }
}
