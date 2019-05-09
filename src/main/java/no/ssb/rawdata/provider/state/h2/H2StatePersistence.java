package no.ssb.rawdata.provider.state.h2;

import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.Single;
import no.ssb.rawdata.api.persistence.CompletedPositionPublisher;
import no.ssb.rawdata.api.persistence.PersistenceException;
import no.ssb.rawdata.api.state.CompletedPosition;
import no.ssb.rawdata.api.state.StatePersistence;
import no.ssb.rawdata.api.util.FileAndClasspathReaderUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Set;

public class H2StatePersistence implements StatePersistence {

    private final H2TransactionFactory transactionFactory;

    public H2StatePersistence(H2TransactionFactory transactionFactory) {
        this.transactionFactory = transactionFactory;
    }

    H2TransactionFactory getTransactionFactory() {
        return transactionFactory;
    }

    @Override
    public void close() throws IOException {
        transactionFactory.close();
    }

    @Override
    public Single<Boolean> expectedPagePositions(String namespace, Set<String> pagePositions) {
        return Single.fromCallable(() -> {
            try (H2Transaction tx = transactionFactory.createTransaction(false)) {
                try {
                    PreparedStatement insert = tx.connection.prepareStatement("INSERT INTO expected_positions (namespace, opaque_id, synthetic_id, batch_ts) VALUES (?, ?, ?, ?)");
                    Timestamp version = new Timestamp(Instant.now().toEpochMilli());
                    int n = 1;
                    for (String pagePosition : pagePositions) {
                        PreparedStatement select = tx.connection.prepareStatement("SELECT count(*) FROM expected_positions WHERE namespace=? AND opaque_id=?");
                        select.setString(1, namespace);
                        select.setString(2, pagePosition);
                        ResultSet selectResultSet = select.executeQuery();
                        if (selectResultSet.next() && selectResultSet.getInt(1) > 0) {
                            continue;
                        }
                        insert.setString(1, namespace);
                        insert.setString(2, pagePosition);
                        insert.setInt(3, n);
                        insert.setTimestamp(4, version);
                        insert.addBatch();
                        n++;
                    }
                    return insert.executeBatch().length == n - 1;
                } catch (SQLException e) {
                    throw new PersistenceException(e);
                }
            }
        });
    }

    @Override
    public Single<Boolean> trackCompletedPositions(String namespace, Set<String> completedPositions) {
        return Single.fromCallable(() -> {
            try (H2Transaction tx = transactionFactory.createTransaction(false)) {
                try {
                    PreparedStatement ps = tx.connection.prepareStatement("INSERT INTO completed_positions (namespace, opaque_id) VALUES (?, ?)");
                    int n = 1;
                    for (String completedPosition : completedPositions) {
                        ps.setString(1, namespace);
                        ps.setString(2, completedPosition);
                        ps.addBatch();
                        n++;
                    }
                    return ps.executeBatch().length == n - 1;
                } catch (SQLException e) {
                    throw new PersistenceException(e);
                }
            }
        });
    }

    @Override
    public Maybe<String> getFirstPosition(String namespace) {
        return Maybe.fromCallable(() -> {
            try (H2Transaction tx = transactionFactory.createTransaction(true)) {
                try {
                    String sql = FileAndClasspathReaderUtils.getResourceAsString("h2/first-position.sql", StandardCharsets.UTF_8);
                    PreparedStatement ps = tx.connection.prepareStatement(sql);
                    ps.setString(1, namespace);
                    ps.setString(2, namespace);
                    ps.setString(3, namespace);
                    ps.setString(4, namespace);
                    ps.setString(5, namespace);
                    ResultSet rs = ps.executeQuery();
                    if (rs.next()) {
                        return rs.getString(2);
                    }
                    return null;
                } catch (SQLException e) {
                    throw new PersistenceException(e);
                }
            }
        });
    }

    @Override
    public Maybe<String> getLastPosition(String namespace) {
        return Maybe.fromCallable(() -> {
            try (H2Transaction tx = transactionFactory.createTransaction(true)) {
                try {
                    String sql = FileAndClasspathReaderUtils.getResourceAsString("h2/last-position.sql", StandardCharsets.UTF_8);
                    PreparedStatement ps = tx.connection.prepareStatement(sql);
                    ps.setString(1, namespace);
                    ps.setString(2, namespace);
                    ps.setString(3, namespace);
                    ps.setString(4, namespace);
                    ps.setString(5, namespace);
                    ps.setString(6, namespace);
                    ResultSet rs = ps.executeQuery();
                    if (rs.next()) {
                        return rs.getString(1);
                    }
                    return null;
                } catch (SQLException e) {
                    throw new PersistenceException(e);
                }
            }
        });    }

    @Override
    public Maybe<String> getNextPosition(String namespace) {
        return Maybe.fromCallable(() -> {
            try (H2Transaction tx = transactionFactory.createTransaction(true)) {
                try {
                    String sql = FileAndClasspathReaderUtils.getResourceAsString("h2/next-position.sql", StandardCharsets.UTF_8);
                    PreparedStatement ps = tx.connection.prepareStatement(sql);
                    ps.setString(1, namespace);
                    ps.setString(2, namespace);
                    ps.setString(3, namespace);
                    ResultSet rs = ps.executeQuery();
                    if (rs.next()) {
                        return rs.getString(2);
                    }
                    return null;
                } catch (SQLException e) {
                    throw new PersistenceException(e);
                }
            }
        });
    }

    @Override
    public Maybe<String> getOffsetPosition(String namespace, String fromPosition, int offset) {
        return Maybe.fromCallable(() -> {
            try (H2Transaction tx = transactionFactory.createTransaction(true)) {
                try {
                    String sql = FileAndClasspathReaderUtils.getResourceAsString("h2/offset-position.sql", StandardCharsets.UTF_8);
                    PreparedStatement ps = tx.connection.prepareStatement(sql);
                    ps.setString(1, namespace);
                    ps.setString(2, namespace);
                    ps.setString(3, namespace);
                    ResultSet rs = ps.executeQuery();
                    if (rs.next()) {
                        return rs.getString(2);
                    }
                    return null;
                } catch (SQLException e) {
                    throw new PersistenceException(e);
                }
            }
        });
    }


    @Override
    public Flowable<CompletedPosition> readPositions(String namespace, String fromPosition, String toPosition) {
        final H2Transaction transaction = transactionFactory.createTransaction(true);
        return Single.fromCallable(() -> {
            try {
                String sql = FileAndClasspathReaderUtils.getResourceAsString("h2/find-positions.sql", StandardCharsets.UTF_8);
                PreparedStatement ps = transaction.connection.prepareStatement(sql);
                ps.setString(1, namespace);
                ps.setString(2, namespace);
                ps.setString(3, namespace);
                ps.setString(4, namespace);
                ps.setString(5, namespace);
                ps.setString(6, fromPosition);
                ps.setString(7, namespace);
                ps.setString(8, toPosition);
                ps.setString(9, namespace);
                ps.setString(10, namespace);
                ps.setString(11, namespace);
                ps.setString(12, fromPosition);
                ps.setString(13, namespace);
                ps.setString(14, toPosition);
                ps.setString(15, namespace);
                ps.setString(16, namespace);
                ResultSet resultSet = ps.executeQuery();
                Deque<CompletedPosition> result = new LinkedList<>();
                while (resultSet.next()) {
                    String position = resultSet.getString(2);
                    result.add(new CompletedPosition(namespace, position));
                }
                return result;
            } catch (SQLException e) {
                throw new PersistenceException(e);
            }
        }).flatMapPublisher(result -> Flowable.fromPublisher(new CompletedPositionPublisher(result))
        );
    }
}
