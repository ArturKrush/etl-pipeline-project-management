db = db.getSiblingDB('mongo_db');

db.createUser({
    user: 'mongo_user',
    pwd: 'mongo_password',
    roles: [{ role: 'readWrite', db: 'mongo_db' }]
});

db.createCollection('source_collection');

db.source_collection.insertMany([
    { "id": 101, "Name": "Global SEO Strategy", "Description": "Expanding to EU markets", "StartDate": "2024-03-01T00:00:00Z", "EndDate": null, "Manager": "Lucas Pierce", "Field": "SEO", "Status": "Active", "Cost": "40000", "Benefit": "150000" },
    { "id": 102, "Name": "  Mobile App V2", "Description": "React Native rewrite", "StartDate": "2024-01-10T00:00:00Z", "EndDate": null, "Manager": "Emily Chen", "Field": "Development", "Status": "Active", "Cost": "120000.50", "Benefit": "250000" },
    { "id": 103, "Name": "99 Problems Project", "Description": "Fixing backlog bugs", "StartDate": "2024-05-01T00:00:00Z", "EndDate": "2024-08-01T00:00:00Z", "Manager": "Marcus Johnson", "Field": "Development", "Status": "Completed", "Cost": "20000", "Benefit": "50000" }, // ПРОЙДЕ (Name стане NULL, але є Desc)
    { "id": 104, "Name": null, "Description": null, "StartDate": "2024-06-15T00:00:00Z", "EndDate": null, "Manager": "Amanda Waller", "Field": "Management", "Status": "Active", "Cost": "5000", "Benefit": "0" }, // БИТИЙ: Немає Name та Desc -> ВИДАЛИТЬСЯ
    { "id": 105, "Name": "Vendor Risk Assessment", "Description": "Auditing 3rd party APIs", "StartDate": "2024-02-01T00:00:00Z", "EndDate": "2024-04-01T00:00:00Z", "Manager": "Robert Jenkins", "Field": "Management", "Status": "Completed", "Cost": "15000", "Benefit": "0" },
    { "id": 106, "Name": "Employee Engagement Survey", "Description": "Annual HR survey", "StartDate": "2024-10-01T00:00:00Z", "EndDate": "2024-11-01T00:00:00Z", "Manager": "Diana Prince", "Field": "Talent Acquisition", "Status": "Active", "Cost": "2000", "Benefit": "10000" },
    { "id": 107, "Name": "Abandoned Prototype", "Description": "Failed test", "StartDate": "2022-01-01T00:00:00Z", "EndDate": null, "Manager": null, "Field": "Development", "Status": null, "Cost": "10000", "Benefit": "0" }, // БИТИЙ: Старий, без менеджера/статусу -> ВИДАЛИТЬСЯ
    { "id": 108, "Name": "Predictive Maintenance ML", "Description": "IoT data pipeline", "StartDate": "2024-07-01T00:00:00Z", "EndDate": null, "Manager": "Steven Wong", "Field": "Data Analysis", "Status": "Suspended", "Cost": "60000", "Benefit": "200000" },
    { "id": 109, "Name": "Sales Training Bootcamp", "Description": null, "StartDate": "2024-08-15T00:00:00Z", "EndDate": "2024-09-15T00:00:00Z", "Manager": "Amanda Waller", "Field": null, "Status": "Completed", "Cost": "15000", "Benefit": "45000" }, // БИТИЙ: Немає Field -> ВИДАЛИТЬСЯ
    { "id": 110, "Name": "Infrastructure as Code", "Description": "Terraform implementation", "StartDate": "2024-04-01T00:00:00Z", "EndDate": null, "Manager": "Marcus Johnson", "Field": "Development", "Status": "Active", "Cost": "45000", "Benefit": "100000" },
    { "id": 111, "Name": "Brand Voice Guidelines", "Description": "Updating corporate tone", "StartDate": "2024-05-10T00:00:00Z", "EndDate": "2024-07-10T00:00:00Z", "Manager": "Lucas Pierce", "Field": "Content Strategy", "Status": "Completed", "Cost": "8000", "Benefit": "20000" },
    { "id": 112, "Name": "Data Privacy Init", "Description": "Anonymizing user data", "StartDate": "2024-09-01T00:00:00Z", "EndDate": null, "Manager": "Steven Wong", "Field": "Data Analysis", "Status": "Active", "Cost": "30000", "Benefit": "0" },
    { "id": 113, "Name": "  Mobile App V2", "Description": "React Native rewrite", "StartDate": "2024-01-10T00:00:00Z", "EndDate": null, "Manager": "Emily Chen", "Field": "Development", "Status": "Active", "Cost": "120000.50", "Benefit": "250000" }, // ПОВНИЙ ДУБЛІКАТ -> ВИДАЛИТЬСЯ
    { "id": 114, "Name": "Q3 Board Presentation", "Description": "Financials reporting", "StartDate": "2024-07-01T00:00:00Z", "EndDate": "2024-07-15T00:00:00Z", "Manager": "Robert Jenkins", "Field": "Management", "Status": "Completed", "Cost": "1000", "Benefit": "0" },
    { "id": 115, "Name": "New Graduate Program", "Description": "University hiring", "StartDate": "2024-05-01T00:00:00Z", "EndDate": null, "Manager": "Diana Prince", "Field": "Talent Acquisition", "Status": "Active", "Cost": "25000", "Benefit": "150000" }
]);
