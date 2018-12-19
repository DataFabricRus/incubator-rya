package org.apache.rya.rdftriplestore.evaluation;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.io.IOUtils;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.persist.RdfDAOException;
import org.apache.rya.api.persist.RdfEvalStatsDAO;
import org.apache.rya.utils.QueryPlanAssert;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.util.Namespaces;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.StrictEvaluationStrategy;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RunWith(Parameterized.class)
public class StaticQueryPlannerTest {

    @Parameterized.Parameters(name = "{0}")
    public static Object[] data() {
        return new Object[]{"test-1", "test-2"};
    }

    private static final String PREFIXES_AS_STRING;
    private static final Map<String, String> PREFIXES;

    static {
        try {
            Model model = Rio.parse(
                    StaticQueryPlannerTest.class.getResourceAsStream(
                            StaticQueryPlannerTest.class.getSimpleName() + "/prefixes.ttl"),
                    "",
                    RDFFormat.TURTLE
            );

            PREFIXES = Namespaces.asMap(model.getNamespaces());
            PREFIXES_AS_STRING = PREFIXES.entrySet().stream()
                    .map(it -> "PREFIX " + it.getKey() + ": <" + it.getValue() + ">\n")
                    .collect(Collectors.joining());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private final String filePrefix;

    public StaticQueryPlannerTest(String filePrefix) {
        this.filePrefix = this.getClass().getSimpleName() + "/" + filePrefix;
    }

    @Test
    public void test() throws Exception {
        String queryFN = this.filePrefix + "-query.sparql";
        String query = PREFIXES_AS_STRING + IOUtils.toString(
                this.getClass().getResourceAsStream(queryFN), StandardCharsets.UTF_8);
        String expectedFN = this.filePrefix + "-expected.qp";
        String expected = IOUtils.toString(this.getClass().getResourceAsStream(expectedFN), StandardCharsets.UTF_8);
        String statsFN = this.filePrefix + "-statistics.json";
        String statsJSON = IOUtils.toString(this.getClass().getResourceAsStream(statsFN), StandardCharsets.UTF_8);

        JsonArray jsonArray = new JsonParser().parse(statsJSON).getAsJsonArray();

        TupleExpr expr = new QueryRoot(toTupleExpr(query));

        new StaticQueryPlanner(createEvaluationStrategy(), createEvaluationStatistics(jsonArray))
                .optimize(expr, null, new QueryBindingSet());

        QueryPlanAssert.assertEquals(expected, expr);
    }

    private TupleExpr toTupleExpr(String query) {
        return new SPARQLParser().parseQuery(query, null).getTupleExpr();
    }

    private EvaluationStrategy createEvaluationStrategy() {
        return new StrictEvaluationStrategy(new TripleSource() {
            @Override
            public CloseableIteration<? extends Statement, QueryEvaluationException> getStatements(
                    Resource subj, IRI pred, Value obj, Resource... contexts) throws QueryEvaluationException {
                return null;
            }

            @Override
            public ValueFactory getValueFactory() {
                return null;
            }
        }, null);
    }

    private EvaluationStatistics createEvaluationStatistics(JsonArray jsonArray) {
        AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();

        return new RyaEvaluationStatistics<>(conf, createStatsDao(jsonArray));
    }

    private <C extends RdfCloudTripleStoreConfiguration> RdfEvalStatsDAO<C> createStatsDao(JsonArray jsonArray) {
        return new RdfEvalStatsDAO<C>() {

            private final Map<String, Double> counts = new HashMap<>();
            private boolean isInitialized = false;

            {
                jsonArray.forEach(it -> {
                    JsonObject jo = it.getAsJsonObject();

                    StringBuilder sb = new StringBuilder();
                    if (jo.has("s")) {
                        sb.append(expandPrefixedUri(jo.get("s").getAsString()));
                    }
                    if (jo.has("p")) {
                        sb.append(expandPrefixedUri(jo.get("p").getAsString()));
                    }
                    if (jo.has("o")) {
                        sb.append(expandPrefixedUri(jo.get("o").getAsString()));
                    }

                    double count = jo.get("count").getAsDouble();

                    counts.put(sb.toString(), count);
                });
            }

            @Override
            public void init() throws RdfDAOException {
                isInitialized = true;
            }

            @Override
            public boolean isInitialized() throws RdfDAOException {
                return isInitialized;
            }

            @Override
            public void destroy() throws RdfDAOException {

            }

            @Override
            public double getCardinality(C conf, CARDINALITY_OF card, List<Value> val) throws RdfDAOException {
                String key = val.stream().map(Value::stringValue).collect(Collectors.joining());

                Double cardinality = counts.get(key);

                return cardinality != null ? cardinality : Double.MAX_VALUE;
            }

            @Override
            public double getCardinality(C conf, CARDINALITY_OF card, List<Value> val, Resource context) throws RdfDAOException {
                String key = val.stream().map(Value::stringValue).collect(Collectors.joining());

                if (context != null) {
                    key += context.stringValue();
                }

                Double cardinality = counts.get(key);

                return cardinality != null ? cardinality : Double.MAX_VALUE;
            }

            @Override
            public void setConf(C conf) {

            }

            @Override
            public C getConf() {
                return null;
            }

            private String expandPrefixedUri(String prefixedUri) {
                if (prefixedUri.startsWith("http://")
                        || prefixedUri.startsWith("https://")
                        || prefixedUri.startsWith("urn:")) {
                    return prefixedUri;
                }

                int colonIdx = prefixedUri.indexOf(':');
                String prefix = prefixedUri.substring(0, colonIdx);

                return PREFIXES.get(prefix) + prefixedUri.substring(colonIdx + 1);
            }
        };
    }
}
