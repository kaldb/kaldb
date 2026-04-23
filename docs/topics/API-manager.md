# Manager API

API definitions for manager nodes, accessed via manager Docs service and admin tools.

For unframed JSON requests and responses, this service follows the protobuf JSON encoding used by
Armeria. Default-valued proto3 fields may be omitted from responses. In practice, that means:
- `usingDedicatedPartitions` may be absent when it is `false`
- `provisionedCapacity` may be absent when it is `0`

<api-doc openapi-path="../api/manager_api.yaml">
    <api-endpoint endpoint="/slack.proto.astra.ManagerApiService/CreateDatasetMetadata" method="POST">
        <request>
            <sample lang="JSON">
                {
                  "name": "indexName",
                  "owner": "Index owner",
                  "serviceNamePattern": "_all"
                }
            </sample>
        </request>
        <response type="200">
            <sample lang="JSON">
                {
                  "name": "indexName",
                  "owner": "Index owner",
                  "serviceNamePattern": "_all"
                }
            </sample>
        </response>
    </api-endpoint>
    <api-endpoint endpoint="/slack.proto.astra.ManagerApiService/GetDatasetMetadata" method="POST">
        <request>
            <sample lang="JSON">
                {
                  "name": "indexName"
                }
            </sample>
        </request>
        <response type="200">
            <sample lang="JSON">
                {
                  "name": "indexName",
                  "owner": "Index owner",
                  "throughputBytes": "12000000",
                  "partitionConfigs": [
                    {
                      "startTimeEpochMs": "1706646163791",
                      "endTimeEpochMs": "1706646250152",
                      "partitions": [
                        "0",
                        "1"
                      ]
                    },
                    {
                      "startTimeEpochMs": "1706646250153",
                      "endTimeEpochMs": "9223372036854775807",
                      "partitions": [
                        "0",
                        "1",
                        "2"
                      ]
                    }
                  ],
                  "serviceNamePattern": "_all"
                }
            </sample>
        </response>
    </api-endpoint>
    <api-endpoint endpoint="/slack.proto.astra.ManagerApiService/ListDatasetMetadata" method="POST">
        <response type="200">
            <sample lang="JSON">
                {
                  "datasetMetadata": [
                    {
                      "name": "example",
                      "owner": "example",
                      "throughputBytes": "4000000",
                      "partitionConfigs": [
                        {
                          "startTimeEpochMs": "1698967727980",
                          "endTimeEpochMs": "1698969132808",
                          "partitions": [
                            "0"
                          ]
                        },
                        {
                          "startTimeEpochMs": "1698969132809",
                          "endTimeEpochMs": "1699035709531",
                          "partitions": [
                            "0"
                          ]
                        },
                        {
                          "startTimeEpochMs": "1699035709532",
                          "endTimeEpochMs": "9223372036854775807",
                          "partitions": [
                            "0",
                            "1"
                          ]
                        }
                      ],
                      "serviceNamePattern": "_all"
                    }
                  ]
                }
            </sample>
        </response>
    </api-endpoint>
    <api-endpoint endpoint="/slack.proto.astra.ManagerApiService/RestoreReplica" method="POST">
        <request> 
            <sample lang="JSON">
                {
                  "serviceName": "example",
                  "startTimeEpochMs": "1713375159221",
                  "endTimeEpochMs": "1713378759221"
                }
            </sample>
        </request>
        <response type="200">
            <sample lang="JSON">
                {
                    "status": "success"
                }
            </sample>
        </response>
    </api-endpoint>
    <api-endpoint endpoint="/slack.proto.astra.ManagerApiService/RestoreReplicaIds" method="POST">
        <request>
            <sample lang="JSON">
                {
                  "idsToRestore": [
                    "f3a94795-71a1-4735-9eb1-60968fbae196"
                  ]
                }
            </sample>
        </request>
        <response type="200">
            <sample lang="JSON">
                {
                    "status": "success"
                }
            </sample>
        </response>
    </api-endpoint>
    <api-endpoint endpoint="/slack.proto.astra.ManagerApiService/UpdateDatasetMetadata" method="POST">
        <request>
            <sample lang="JSON">
                {
                  "name": "example",
                  "owner": "Updated owner",
                  "serviceNamePattern": "_all"
                }
            </sample>
        </request>
        <response type="200">
            <sample lang="JSON">
                {
                  "name": "example",
                  "owner": "Updated owner",
                  "serviceNamePattern": "_all"
                }
            </sample>
        </response>
    </api-endpoint>
    <api-endpoint endpoint="/slack.proto.astra.ManagerApiService/UpdatePartitionAssignment" method="POST">
        <request>
            <sample lang="JSON" title="Manual assignment">
                {
                  "name": "example",
                  "throughputBytes": "4000000",
                  "partitionIds": [
                    "0",
                    "1"
                  ]
                }
            </sample>
            <sample lang="JSON" title="Auto-assignment on shared partitions">
                {
                  "name": "example",
                  "throughputBytes": "4000000",
                  "partitionIds": []
                }
            </sample>
            <sample lang="JSON" title="Auto-assignment on dedicated partitions">
                {
                  "name": "payments",
                  "throughputBytes": "12000000",
                  "partitionIds": [],
                  "requireDedicatedPartition": true
                }
            </sample>
        </request>
        <response type="200">
            <sample lang="JSON">
                {
                  "assignedPartitionIds": [
                    "0",
                    "1"
                  ]
                }
            </sample>
        </response>
    </api-endpoint>
    <api-endpoint endpoint="/slack.proto.astra.ManagerApiService/CreatePartition" method="POST">
        <request>
            <sample lang="JSON">
                {
                  "partitionId": "3",
                  "maxCapacity": "250"
                }
            </sample>
        </request>
        <response type="200">
            <sample lang="JSON">
                {
                  "partitionId": "3",
                  "maxCapacity": "250"
                }
            </sample>
        </response>
    </api-endpoint>
    <api-endpoint endpoint="/slack.proto.astra.ManagerApiService/ListPartitionMetadata" method="POST">
        <response type="200">
            <sample lang="JSON">
                {
                  "partitionMetadata": [
                    {
                      "partitionId": "1",
                      "maxCapacity": "250",
                      "provisionedCapacity": "100",
                      "shared": {
                        "datasets": [
                          "example"
                        ]
                      }
                    },
                    {
                      "partitionId": "2",
                      "maxCapacity": "250",
                      "provisionedCapacity": "120",
                      "dedicated": {
                        "dataset": "payments"
                      }
                    },
                    {
                      "partitionId": "3",
                      "maxCapacity": "250",
                      "empty": {}
                    }
                  ]
                }
            </sample>
        </response>
    </api-endpoint>
    <api-endpoint endpoint="/slack.proto.astra.ManagerApiService/DeletePartition" method="POST">
        <request>
            <sample lang="JSON">
                {
                  "partitionId": "3"
                }
            </sample>
        </request>
        <response type="200">
            <sample lang="JSON">
                {
                  "status": "Deleted partition 3 successfully"
                }
            </sample>
        </response>
    </api-endpoint>
</api-doc>
