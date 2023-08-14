package dgb.dsi.pension_data.batchprocessing;

import dgb.dsi.pension_data.entity.Person;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {



    @Bean
        public FlatFileItemReader<Person> reader() {
         return  new FlatFileItemReaderBuilder<Person>()
                 .name("personItemReader")
                 .resource(new ClassPathResource("simple-data.csv"))
                 .delimited()
                 .names(new String[]{"firstName","lastName"})
                 .fieldSetMapper(new BeanWrapperFieldSetMapper<>(){{
                         setTargetType(Person.class);
                     }})
                  .build();
        }
        @Bean
        public  PersonItemProcessor personItemProcessor(){

            return  new PersonItemProcessor();

        }

    @Bean
    public JdbcBatchItemWriter<Person> writer(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Person>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("INSERT INTO people (first_name, last_name) VALUES (:firstName, :lastName)")
                .dataSource(dataSource)
                .build();
    }

    @Bean
    public Job importUserJob(JobRepository jobRepository,JobCompletionNotificationListener  jobCompletionNotificationListener,
                             Step step1){
        return  new JobBuilder("importUserJob",jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(jobCompletionNotificationListener)
                .flow(step1)
                .end()
                .build();
    }
    @Bean
    public  Step step(JobRepository jobRepository,PlatformTransactionManager platformTransactionManager
    , JdbcBatchItemWriter<Person> writer){

  return  new StepBuilder("step1", jobRepository)
          .<Person,Person>chunk(10,platformTransactionManager)
          .reader(reader())
          .processor(personItemProcessor())
          .writer(writer)
         .build();
    }

}
