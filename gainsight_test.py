import requests

url = "https://compliancerisks.eu.gainsightcloud.com/v1/data/objects/query/Activity_Timeline"

headers = {
    "AccessKey": "7bccbd05-51e9-4a07-a423-a0a851ca6fc2",
    "Content-Type": "application/json"
}

body = {
    "select": [
        "Gsid",
        "CreatedDate",
        "contextname",
        "GsRelationshipId",
        "GsCompanyId",
        "AuthorId",
        "InternalAttendees",
        "ExternalAttendees",
        "internalAttendees",
        "ActivityDate",
        "Ant__Category__c",
        "ContextName",
        "Ant__Customer_Journey_Stage__c",
        "Ant__Duration_in_hours__c",
        "Ant__Initiative_Type__c",
        "DurationInMins",
        "GsCompanyId",
        "ExternalSource",
        "GsRelationshipTypeId",
        "InternalAttendees",
        "LastModifiedBy",
        "LastModifiedDate",
        "Milestone_type_name",
        "MilestoneType",
        "NotesPlainText",
        "ReportingCategory",
        "Scorecard",
        "ScorecardMeasure",
        "ScoreDetails",
        "Source",
        "SfTaskId",
        "SfEventId",
        "SfdcCtaId",
        "UserEmail",
        "UserId",
        "UserName",
        "CreatedBy",
        "CreatedDate"

    ],
    "orderBy": {
        "CreatedDate": "desc"
    },
    "limit": 10,
    "offset": 0
}

response = requests.post(url, headers=headers, json=body)
print(response.status_code)
print(response.json())



