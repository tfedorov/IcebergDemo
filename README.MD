# IcebergDemo Project

Slideshare presentation [Harry Potter & Apache iceberg format](https://www.slideshare.net/TarasFedorov/harry-potter-apache-iceberg-formatpptx).

This is a simple standalone Scala application that demonstrates working
with [Apache Iceberg](https://iceberg.apache.org/).

It includes functionality for creating a database, managing tables, inserting data, updating data (upsert), and
performing time travel queries. The project uses a Harry Potter character dataset
from [Kaggle](https://www.kaggle.com/datasets/electroclashh/harry-potter-dataset/data)
The app stores data locally inside a resource folder. The project created for demonstration purposes only.

## Prerequisites

Before you begin, ensure you have met the following requirements:

- JVM is installed. Version 11 (or less)
- Scala is installed. Version (2.12)
- Gradle is installed. Version 8.2.1 (or less)

## Usage

Launch the app via IDE or console command.

```bash
.gradle run 
```

## Functionality

- Create a Database and Table locally.
- Initial Data Insertion: The application can insert data from the Kaggle dataset into the table.
- Upsert Data (Updating): You can perform upsert operations to update existing data in the table.
- Table Management/Time Travel: This feature allows you to manage tables and perform time travel queries to see
  historical data versions.

Feel free to explore the code and adapt it for your own use cases. If you have any questions or encounter issues, please
don't hesitate to open an issue on this GitHub repository.

Enjoy working with Apache Iceberg!
