import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;
import java.util.List;

public class Test_Data_Flow {
    public static void main(String args[]){
        System.out.println("First Try");

        TableSchema schema = new TableSchema().setFields(Arrays.asList(new TableFieldSchema().setName("CUST_TIER_CODE").setType("STRING").setMode("REQUIRED"),
                new TableFieldSchema().setName("SKU").setType("INTEGER").setMode("REQUIRED"),
                new TableFieldSchema().setName("total_no_of_product_views").setType("INTEGER").setMode("REQUIRED")
        ));

        TableSchema schema1 = new TableSchema().setFields(Arrays.asList(new TableFieldSchema().setName("CUST_TIER_CODE").setType("STRING").setMode("REQUIRED"),
                new TableFieldSchema().setName("SKU").setType("INTEGER").setMode("REQUIRED"),
                new TableFieldSchema().setName("TOTAL_SALES_AMOUNT").setType("FLOAT").setMode("REQUIRED")
        ));
        DataflowPipelineOptions pipelineOptions = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        pipelineOptions.setJobName("firsttestnad4");
        pipelineOptions.setProject("york-cdf-start");
        pipelineOptions.setRegion("us-central1");
        pipelineOptions.setRunner(DataflowRunner.class);
        //pipelineOptions.setTempLocation("gs://nm_york_cdf-_start/results/tmp");


        Pipeline pipeline = Pipeline.create(pipelineOptions);

//        final List<String> input = Arrays.asList("and a one", "and a two", "and a one, two, three, four");
//        pipeline.apply(Create.of(input)).apply(TextIO.write().to("gs://nm_york_cdf-_start/results/tmp/java").withSuffix(".txt"));
        PCollection<TableRow> rows =
                pipeline.apply(
                        "Read from BigQuery query",
                        BigQueryIO.readTableRows()
                                .fromQuery("select cust.CUST_TIER_CODE,product.SKU, count(*) as total_no_of_product_views from `york-cdf-start.final_input_data.customers` cust join `york-cdf-start.final_input_data.product_views` product on product.CUSTOMER_ID = cust.CUSTOMER_ID group by cust.CUST_TIER_CODE,product.SKU")
                                .usingStandardSql());

        PCollection<TableRow> rows1 =
                pipeline.apply(
                        "Read from BigQuery query",
                        BigQueryIO.readTableRows()
                                .fromQuery("select cust.CUST_TIER_CODE,orders.SKU,sum(orders.ORDER_AMT) as TOTAL_SALES_AMOUNT from `york-cdf-start.final_input_data.customers` cust join `york-cdf-start.final_input_data.orders` orders on cust.CUSTOMER_ID = orders.CUSTOMER_ID group by cust.CUST_TIER_CODE,orders.SKU")
                                .usingStandardSql());
        rows.apply(
                "Write to BigQuery",
                BigQueryIO.writeTableRows()
                        .to(String.format("york-cdf-start:final_nadhiya_mathialagan.customerProductView"))
                        .withSchema(schema)

                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)

                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

        rows1.apply(
                "Write to BigQuery",
                BigQueryIO.writeTableRows()
                        .to(String.format("york-cdf-start:final_nadhiya_mathialagan.customerProductSales"))
                        .withSchema(schema1)

                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)

                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

        pipeline.run().waitUntilFinish();
    }
}
