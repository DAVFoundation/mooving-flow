package org.dav.vehicle_rider.rider_payment;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.cassandra.CassandraIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.flink.api.java.utils.ParameterTool;
import org.dav.config.Config;
import org.dav.vehicle_rider.cassandra.DavConversionRate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HttpsURLConnection;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.URL;
import java.util.Arrays;
import java.util.List;


public class DavConversionRates {

    private static final Logger LOG = LoggerFactory.getLogger(DavConversionRates.class);
    private static final String CASSANDRA_SCEHME_NAME = "vehicle_rider";
    private static final List<String> SUPPORTED_CURRENCIES =  Arrays.asList( "USD", "ILS" );

    public static void main(String[] args) {

        ParameterTool parameters = ParameterTool.fromArgs(args);
        String apiKey = parameters.get("CMC_API_KEY");
        Config config = Config.create(false,args);
        Pipeline pipeline = Pipeline.create(config.pipelineOptions());

        runSetConversionRates(config, pipeline, apiKey);

        pipeline.run();

    }

    private static void runSetConversionRates(Config config, Pipeline pipeline, String cmcApiKey) {

        PCollection<String> currencies = pipeline.apply("get supported currencies",
                Create.of(SUPPORTED_CURRENCIES)).setCoder(StringUtf8Coder.of());

        GetConversionRates getConversionRates = new GetConversionRates(cmcApiKey, LOG);

        PCollection<DavConversionRate> conversionRates = currencies
                .apply("get conversion rates", ParDo.of(getConversionRates))
                .apply("Filter NULLs", Filter.by(obj -> obj != null));

        conversionRates.apply("write conversion rates to cassandra",
                CassandraIO.<DavConversionRate>write().withHosts(Arrays.asList(config.cassandraSeed()))
                        .withPort(config.cassandraPort()).withKeyspace(CASSANDRA_SCEHME_NAME)
                        .withEntity(DavConversionRate.class));

    }

}

class GetConversionRates extends DoFn<String, DavConversionRate> {

    private static final long serialVersionUID = 1L;

    private final Logger _log;
    private HttpsURLConnection _connection;
    private static final String CMC_API_CALL_URL = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest?symbol=DAV&convert=%s";
    private final String _apiKey;

    public GetConversionRates(String apiKey, Logger log) {
        _log = log;
        _apiKey = apiKey;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        String currency = context.element();

        String fullUrl = String.format(CMC_API_CALL_URL, currency);
        try {
            URL obj = new URL(fullUrl);
            _connection = (HttpsURLConnection) obj.openConnection();
            _connection.setRequestMethod("GET");
            _connection.setRequestProperty("Content-Type", "application/json");
            _connection.setRequestProperty("X-CMC_PRO_API_KEY", _apiKey);
            int status = _connection.getResponseCode();
            if (status == 200) {
                BufferedReader in = new BufferedReader(new InputStreamReader(_connection.getInputStream()));
                String inputLine;
                StringBuffer content = new StringBuffer();
                while ((inputLine = in.readLine()) != null) {
                    content.append(inputLine);
                }
                in.close();
                String response = content.toString();
                JsonElement jelement = new JsonParser().parse(response);
                JsonObject jobject = jelement.getAsJsonObject();
                jobject = jobject.getAsJsonObject("data");
                jobject = jobject.getAsJsonObject("DAV");
                jobject = jobject.getAsJsonObject("quote");
                jobject = jobject.getAsJsonObject(currency);
                BigDecimal price = jobject.get("price").getAsBigDecimal();
                DavConversionRate conversionRate = new DavConversionRate(currency, price);
                context.output(conversionRate);
                _log.info(String.format("got response from coinmarketcap %s DAV rate is %s", currency, price));
            } else {
                _log.info(String.format("got response from coinmarketcap api for url %s, status code: %s", fullUrl, status));
                context.output(null);
            }
            _connection.disconnect();
        } catch (Exception ex) {
            _log.error(String.format("error while trying to get conversion rates for url: %s", fullUrl), ex);
            context.output(null);
        }
    }
}
