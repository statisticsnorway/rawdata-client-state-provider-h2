package no.ssb.rawdata.provider.state.h2;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import no.ssb.config.DynamicConfiguration;
import no.ssb.rawdata.api.state.StatePersistence;
import no.ssb.rawdata.api.state.StatePersistenceInitializer;
import no.ssb.rawdata.api.util.FileAndClasspathReaderUtils;
import no.ssb.service.provider.api.ProviderName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;

@ProviderName("h2")
public class H2StatePersistenceInitializer implements StatePersistenceInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(H2StatePersistenceInitializer.class);

    @Override
    public String providerId() {
        return "h2";
    }

    @Override
    public Set<String> configurationKeys() {
        return Set.of("database.h2.url");
    }

    @Override
    public StatePersistence initialize(DynamicConfiguration configuration) {
        LOG.info("JdbcURL: {}", configuration.evaluateToString("database.h2.url"));
        HikariDataSource dataSource = openDataSource(configuration.asMap());
        return new H2StatePersistence(new H2TransactionFactory(dataSource));
    }

    public static HikariDataSource openDataSource(Map<String, String> configuration) {
        String jdbcDriverURL = configuration.get("database.h2.url");
        HikariDataSource dataSource = H2StatePersistenceInitializer.openDataSource(
                jdbcDriverURL,
                "sa",
                "sa",
                "rawdata"
        );
        return dataSource;
    }

    // https://github.com/brettwooldridge/HikariCP
    static HikariDataSource openDataSource(String jdbcUrl, String username, String password, String database) {
        Properties props = new Properties();
        props.setProperty("jdbcUrl", jdbcUrl);
        props.setProperty("username", username);
        props.setProperty("password", password);
        props.put("dataSource.logWriter", new PrintWriter(System.out));

        HikariConfig config = new HikariConfig(props);
        config.setAutoCommit(false);
        config.setMaximumPoolSize(10);
        HikariDataSource datasource = new HikariDataSource(config);

        dropOrCreateDatabase(datasource);

        return datasource;
    }

    static void dropOrCreateDatabase(HikariDataSource datasource) {
        try {
            String initSQL = FileAndClasspathReaderUtils.readFileOrClasspathResource("h2/init-db.sql");
//            System.out.printf("initSQL: %s%n", initSQL);
            Connection conn = datasource.getConnection();
            conn.beginRequest();

            try (Scanner s = new Scanner(initSQL)) {
                s.useDelimiter("(;(\r)?\n)|(--\n)");
                try (Statement st = conn.createStatement()) {
                    try {
                        while (s.hasNext()) {
                            String line = s.next();
                            if (line.startsWith("/*!") && line.endsWith("*/")) {
                                int i = line.indexOf(' ');
                                line = line.substring(i + 1, line.length() - " */".length());
                            }

                            if (line.trim().length() > 0) {
                                st.execute(line);
                            }
                        }
                        conn.commit();
                    } finally {
                        st.close();
                    }
                }
            }
            conn.endRequest();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
