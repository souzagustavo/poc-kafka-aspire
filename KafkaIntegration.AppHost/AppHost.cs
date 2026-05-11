var builder = DistributedApplication.CreateBuilder(args);

var kafka = builder.AddKafka("kafka")
    .WithDataVolume("kafka-data")    
    .WithKafkaUI();

var producer = builder.AddProject<Projects.KafkaIntegration_Producer>("producer")
       .WithReference(kafka)
       .WaitFor(kafka)
       .WithReplicas(1);

builder.AddProject<Projects.KafkaIntegration_Consumer>("consumer")
       .WithReference(kafka)
       .WaitFor(producer)
       .WithReplicas(4);

builder.Build().Run();