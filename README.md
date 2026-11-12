
## 1. Project Description

### 1.1 Context and Objectives (Batch to Streaming Transition)

This project is Workshop 3 of the ETL course in the Data Engineering and Artificial Intelligence program (Faculty of Engineering and Basic Sciences, Universidad Autónoma de Occidente). It focuses on the transition from traditional batch ETL pipelines to event-driven streaming pipelines.

Earlier in the course, the work centered on batch processing: ETL pipelines in Python, data cleaning and transformation, data quality, dimensional modeling, Apache Airflow, batch orchestration, and data warehouses. This workshop extends that foundation toward streaming data pipelines, Apache Kafka, event-driven processing, real-time machine learning inference, and streaming analytics.

The goal is to design and implement a streaming ETL pipeline capable of generating real-time predictions of the World Happiness score using Apache Kafka and a pre-trained regression model. The emphasis is on pipeline integration, reproducibility, reliability, and data consistency rather than on maximizing model accuracy.

The project covers the following objectives: integrating heterogeneous datasets into a unified analytical schema, building a batch ETL pipeline for machine learning preparation, training and serializing a regression model, implementing a Kafka producer that streams events, implementing a Kafka consumer that performs real-time inference, defining and validating an event contract before prediction, storing prediction results in a database, and building analytical visualizations from those results.

Because the source data consists of bounded historical CSV files, the streaming behavior is simulated. The planned producer will read the historical records one by one and publish them as events with a configurable delay, so that they arrive over time as if they were generated live.

**Current status:** The batch exploratory stage (analysis, cleaning, and harmonization of the 2015–2019 files) is **implemented in the notebook**. The model, the Kafka components, the database, and the dashboard are **planned and not yet implemented**.

### 1.2 Technologies Used

**Implemented (batch stage):** Python with pandas and NumPy for data manipulation, Matplotlib and Seaborn for visualization, and pathlib for path management. The analysis is documented in a Jupyter notebook. Markdown tables in the notebook are generated with tabulate.

**Planned:**
- **Machine learning:** scikit-learn, with the regression model and its preprocessing packaged in a reproducible Pipeline and serialized to `model.pkl`.
- **Streaming:** Apache Kafka as the event broker, with a Python producer and a Python consumer. The recommended local environment is a single broker deployed with Docker Compose.
- **Storage:** a relational database. PostgreSQL is the recommended option, with SQLite as an alternative for local implementation. SQL scripts will be kept in `sql/`.
- **Dashboard:** one of the suggested tools (Power BI, Looker Studio, or Tableau), querying the prediction database rather than CSV files.
- **Delivery:** GitHub repository, with dependencies listed in `requirements.txt`.

---

## 2. System Architecture

The system is organized into two flows that share a trained model: an offline flow that prepares data and produces the model, and an online flow that processes events and generates predictions. A third stage stores results for analysis.

**Status of this section:** The architecture is a **design**. Only the offline data preparation stage (Section 4) is implemented in the notebook.

### 2.1 Offline Flow (Preparation and Training)

  ```mermaid
graph TD
    A["Historical CSV files (data/raw/)"] --> B["EDA and Harmonization (notebooks/eda.ipynb)"]
    B --> C["Unified Dataset (data/processed/)"]
    C --> D["Feature Engineering and Model Training (notebooks/model_training.ipynb)"]
    D --> E["Serialized Model (models/model.pkl)"]
  ``` 
**Status:** Data profiling, cleaning, and schema harmonization are **implemented in the notebook**. Feature engineering, model training, and serialization of `model.pkl` are **planned**.

The offline flow starts from the historical CSV files for 2015 through 2019. These files are profiled through exploratory data analysis, cleaned, and harmonized into a single schema. Feature engineering will then select and prepare the variables used by the model. A simple regression model will be trained on the prepared data, and the resulting preprocessing steps and model will be saved together as a single serialized artifact, `model.pkl`.

Saving preprocessing and model as one pipeline is essential: the online flow must apply exactly the same transformations used during training, otherwise predictions would be inconsistent with the training data.

### 2.2 Online Flow (Streaming ETL and Inference)

**Status:** **Planned.** No Kafka producer, consumer, or topic has been implemented yet.

The planned online flow simulates real-time data arrival. A Kafka producer will read the prepared historical data record by record, serialize each record as JSON, and publish it to the `happiness-events` topic, using the country as the message key. The topic is planned with three partitions and a replication factor of one, which fits the local single-broker environment.

A Kafka consumer, running in the consumer group `happiness-ml-team`, will receive each event and process it in a fixed sequence. The consumer will first store the original message in the raw table `raw_happiness_events`, before any transformation or prediction, so that the exact input is always preserved. It will then validate the event against the event contract. Valid events will go through the same feature ordering and preprocessing used during training, the serialized model will be loaded, a prediction will be generated, and the result will be stored in the database.

Invalid records are designed to be handled without interrupting the stream. They will remain in the raw table with an explicit processing status, such as `INVALID_SCHEMA` or `INVALID_VALUES`, and they will be excluded from prediction. Errors during prediction will be marked as `PREDICTION_ERROR`. In every case the pipeline is designed to continue processing subsequent events.

The event contract, including the transformation of the `family` field, is described in Section 7.

### 2.3 Storage and Analytics Flow

**Status:** **Planned.** The database schema and dashboard have not been created.

The database is designed to separate raw traceability from analytical reporting. The raw table `raw_happiness_events` will hold every event as received, together with its processing status. The fact table `fact_predictions` will store one row per successful prediction, including the actual happiness score, the predicted score, the prediction error, and the event and prediction timestamps. Each prediction will be linked to its originating raw event through `raw_event_id`, making every result traceable to the exact message that produced it. The dimension tables `dim_country` and `dim_date` will provide the descriptive context needed for analysis.

The dashboard will read from this database and compute the key performance indicators: average prediction error, predictions by country, predicted versus actual scores, and prediction trends over time.

---

## 3. Repository Structure

### 3.1 Modular Organization (`data/`, `notebooks/`, `kafka/`, `sql/`, etc.)

The repository is organized by responsibility so that each stage of the pipeline can be located, reviewed, and rerun independently.

| Path | Purpose | Status |
|---|---|---|
| `data/raw/` | Original historical CSV files for 2015 through 2019. These are not modified. | Implemented |
| `data/processed/` | Outputs of the batch ETL, including the unified dataset `happiness_unified.csv`. | Export defined in `eda.ipynb`; file to be verified in the repository |
| `data/streaming/` | Location reserved for streaming-related artifacts, as recommended by the workshop structure. | Planned |
| `notebooks/` | `eda.ipynb` for exploration, cleaning, and harmonization; `model_training.ipynb` for training and evaluation. | `eda.ipynb` implemented; `model_training.ipynb` planned |
| `kafka/` | `producer.py` (replays historical records as events) and `consumer.py` (validates events and performs inference). | Planned |
| `models/` | Serialized artifact `model.pkl`, containing preprocessing and the regression model. | Planned |
| `sql/` | `create_tables.sql` for the schema and `kpis.sql` for dashboard queries. | Planned |
| `dashboards/` | Dashboard files and screenshots. | Planned |
| `docker-compose.yml` | Definition of the local Kafka environment. | Planned |
| `requirements.txt` | Python dependencies required to reproduce the project. | Planned |
| `README.md` | Project documentation. | In progress |

Paths in the notebook are resolved relative to the project root rather than hard-coded, which allows the project to run from different working directories. Keeping raw and processed data separate preserves the original sources at all times, so every processed output can be regenerated from them.

---

## 4. Data Cleaning Decisions

### 4.1 Exploratory Data Analysis (EDA) of Historical Datasets

**Status:** **Implemented** in `notebooks/eda.ipynb`.

The exploratory analysis covers the five World Happiness datasets for 2015 through 2019. The files contain 158, 157, 155, 156, and 156 records respectively, for a total of 782 observations. The purpose was to identify quality problems and structural differences between the years before defining any harmonization rules.

The analysis examined missing values, duplicated records, inconsistent column names, inconsistent data types, schema differences between years, and potential outliers.

**Missing values.** The only missing value in the original files is in the corruption variable (named "Perceptions of corruption" in the 2018 file) for United Arab Emirates, representing approximately 0.64% of that year's records for this variable. The 2015, 2016, 2017, and 2019 files had no missing values in the analyzed columns. Missing values in `region` for 2017 to 2019 are not present in the original files; they arise during harmonization because that column does not exist in those sources (see Section 4.2).

**Duplicated records.** No fully duplicated rows and no country appearing more than once within the same year were found. The rankings do contain four shared positions, all of which involve identical happiness scores, so they are interpreted as ties rather than duplicated information:
- **2015:** Jordan and Montenegro share position 82 (score 5.192).
- **2016:** Saudi Arabia and Taiwan share position 34 (score 6.379); Poland and South Korea share position 57 (score 5.835); Burkina Faso and Uganda share position 145 (score 3.739).

**Inconsistent column names and schema.** The same concept appears under different labels across years, such as "Economy (GDP per Capita)" in 2015, "Economy..GDP.per.Capita." in 2017, and "GDP per capita" in 2018 and 2019. The 2015 and 2016 files include a `Region` column, which is absent from 2017 onward. These differences confirm that the datasets cannot be concatenated directly.

**Potential outliers.** Outliers were detected with the interquartile range (IQR) method, using the limits Q1 − 1.5 × IQR and Q3 + 1.5 × IQR, applied to each numeric variable within each yearly file. The analysis produced 129 outlier flags. This figure is an accumulation of detections per variable and year, not a count of unique records: the same country can be flagged in several variables or years, so 129 flags do not correspond to 129 distinct observations. Most flags concern corruption perception, generosity, and social support. Flags also appear for `Standard Error` and `Dystopia Residual` in specific years. Negative values were checked in the core variables, and none were found.

**Complementary columns.** The following columns are often confused and are treated differently in this project:
- **Standard Error** (2015 file only): a measure of uncertainty associated with the happiness score. It is not a predictor and is excluded from the unified schema (see Section 4.2).
- **Dystopia Residual** (2015, 2016, and 2017 files): a component of the score decomposition used in the source reports, representing the residual part of the score not explained by the listed factors. It is not a socioeconomic indicator, it is not present in the 2018–2019 files, and it is excluded from the unified schema.
- **Whisker columns** Additional columns (`Whisker.high` and `Whisker.low`) included in the 2017 source dataset. They are excluded from the unified schema because they are not consistently available across all years
- **Whiskers in the EDA boxplots:** the whiskers drawn in the boxplots generated during the EDA are a visual element of the IQR rule, extending to the most extreme values within 1.5 × IQR. They are not data columns and are not the same element as the `Whisker.high` and `Whisker.low` columns above.

### 4.2 Schema Harmonization and Unification Strategy

**Status:** The harmonization of the five files into a unified dataset is **implemented in the notebook**. The export to `data/processed/happiness_unified.csv` is **defined in the notebook**, and its repository-level verification is **pending**. The modeling strategy in the final subsection is a **proposal pending methodological validation**.

The unified analytical schema contains eleven standardized columns: `country`, `year`, `happiness_rank`, `happiness_score`, `gdp`, `social_support`, `health`, `freedom`, `generosity`, `corruption`, and `region`. Each source column is mapped to its unified name through an explicit, per-year dictionary:

| Unified column | 2015–2016 source | 2017 source | 2018–2019 source |
|---|---|---|---|
| `country` | Country | Country | Country or region |
| `happiness_rank` | Happiness Rank | Happiness.Rank | Overall rank |
| `happiness_score` | Happiness Score | Happiness.Score | Score |
| `gdp` | Economy (GDP per Capita) | Economy..GDP.per.Capita. | GDP per capita |
| `social_support` | Family | Family | Social support |
| `health` | Health (Life Expectancy) | Health..Life.Expectancy. | Healthy life expectancy |
| `freedom` | Freedom | Freedom | Freedom to make life choices |
| `generosity` | Generosity | Generosity | Generosity |
| `corruption` | Trust (Government Corruption) | Trust..Government.Corruption. | Perceptions of corruption |
| `region` | Region | Not available | Not available |

An explicit mapping was chosen over automatic name matching because the differences between years are not always predictable, and a manual mapping keeps each correspondence visible and auditable. After renaming, each dataset is checked for the expected columns, the `year` field is added as an identifier, and columns are ordered according to the unified schema. The five datasets are then concatenated into a single dataset of 782 rows and eleven columns.

The notebook contains an export step that writes this dataset to `data/processed/happiness_unified.csv` and a final validation cell that reports its checks. The validation printed in the notebook reports 782 rows, 11 columns, no duplicated country-year records, no missing target values, one missing corruption value, and 467 records labeled as region not available in the source. These results should be reproduced by re-running the notebook, and the presence of the exported file in the repository should be verified before the README is finalized.

**Excluded columns.** `Standard Error` (2015 only), `Dystopia Residual` (2015–2017 only), and the `Whisker.high` and `Whisker.low` columns (2017 only) are not part of the unified schema. They do not appear consistently across all years, and they do not have a comparable counterpart in every file. Including them would introduce structural missing values for most of the data. The exclusion is documented here so that it is a visible decision rather than an omission.

**Structural missing region.** The `region` variable does not exist in the 2017 to 2019 source files. When those files are harmonized into the unified schema, `region` is therefore empty for their records. This absence is structural rather than an error in the data, so it is not imputed. Those records are labeled "Not available in source" in the processed output.

**Comparability between periods.** The harmonization above is a *name-level* mapping. It establishes that two columns correspond to a similar concept in the same position of the file, but it does not establish that they are methodologically equivalent measurements. This distinction matters for the following reasons:
- The 2015–2017 files report the contributions of factors (economy, family, health, freedom, generosity, and trust) to the happiness score, which together with the residual term add up to the score.
- The 2018–2019 files report explanatory indicators, such as GDP per capita, social support, healthy life expectancy, freedom to make life choices, and perceptions of corruption. These may have different definitions and scales from the earlier contribution-based variables.

For this reason, a column such as `social_support` (mapped from "Family" in 2015–2017 and "Social support" in 2018–2019) should not be treated as the same measurement across all years without validation. The same caution applies to every harmonized variable. The unified dataset is appropriate for integration and exploratory analysis, but the comparability of its variables across the two periods has not been validated against the original reports.

**Proposed modeling strategy.** The unified dataset for 2015–2019 is kept for integration and EDA. As a proposed strategy, the first regression model would be trained only on the 2018–2019 records (312 observations, 156 per year), because these files share a more homogeneous set of explanatory indicators. This choice is a **proposal pending methodological validation**: the definitions of the variables in 2018 and 2019 must be checked against the original World Happiness Report documentation before it is adopted. The strategy has **not yet been implemented or evaluated**.

### 4.3 Treatment of Null, Duplicate, and Outlier Values

**Null values.** The single missing value in the original files is the corruption value for United Arab Emirates in 2018, and it is preserved as null. It was not replaced with zero or with a statistical estimate, because the available information is insufficient to determine its real value, and any substitute would introduce artificial data. Imputation is deferred. It is planned to be evaluated inside the machine learning preprocessing pipeline and fitted on training data only, to avoid leakage from the test set. **This evaluation is pending.** The structural missing values in `region` (2017–2019) are not original nulls. They arise from the absence of that column in those files and are handled as described in Section 4.2.

**Duplicate records.** No rows were removed. The EDA found no fully duplicated records and no repeated country-year combinations, so `drop_duplicates()` would not remove any real duplicates. The four shared ranking positions are ties and are retained with their original positions. The validation cell in the notebook reports zero duplicated country-year records in the unified dataset.

**Outliers.** Observations flagged by the IQR method are preserved. The IQR rule identifies values that are statistically unusual relative to their distribution within a given file; it does not, by itself, demonstrate that a value is an error. Because the flags cannot be classified as errors on this basis alone, removing them would risk discarding valid observations. For the same reason, no automatic capping or replacement was applied. Whether any flagged value is a data-entry error or a genuine extreme remains an open question for contextual review. The year-specific variables that produced flags, `Standard Error` and `Dystopia Residual`, are not carried into the unified schema and therefore do not enter the model.

**Variable roles.** `happiness_score` is the target variable. The proposed predictors are  `gdp`, `social_support`, `health`, `freedom`, `generosity`, and `corruption`. The identifiers are `country` and `year`, and `happiness_rank` and `region` are kept as descriptive variables. `happiness_rank` is retained for descriptive analysis but excluded from the predictors, because it is derived from the same score the model predicts and would constitute target leakage.

**Validation of the processed dataset.** The validation cell in the notebook checks the structural integrity of the processed dataset: row and column counts, absence of duplicated country-year records, absence of missing target values, the single preserved corruption null, and the region labels described above. These checks confirm structural integrity only. They do not validate the comparability of variables across periods, which is addressed in Section 4.2.

---


## 5. Feature Engineering Decisions

### 5.1 Variable Selection and Justification

**Status:** **Implemented** in `notebooks/model_training.ipynb`.

The modeling dataset is restricted to the 2018 and 2019 records of the unified dataset, which contains 312 observations (156 per year). The restriction responds to methodological differences between earlier and recent editions of the World Happiness Report. The 2015–2017 files report contributions of factors to the happiness score, whereas the 2018–2019 files report explanatory indicators with different definitions and scales (see Section 4.2). Selecting the two most recent years reduces the risk of combining variables that do not share a common definition. This selection is an analytical decision, and it does not imply that all editions of the report are directly comparable. The definitions of the 2018 and 2019 variables are still pending validation against the original report documentation.

The target variable is `happiness_score`. The predictor variables are:

- `gdp`
- `social_support`
- `health`
- `freedom`
- `generosity`
- `corruption`

These six variables are the numerical explanatory indicators available in the unified schema. The variables `country`, `year`, and `region` are excluded because they are identifiers or descriptive attributes, not explanatory measures. The variable `happiness_rank` is excluded to avoid leakage of information related to the target, since it is a position derived from the same score that the model predicts (see Section 5.3).

### 5.2 Preprocessing for Machine Learning

**Status:** **Implemented** in `notebooks/model_training.ipynb`.

**Data split.** The modeling dataset is divided into 70% for training and 30% for testing, using `train_test_split` with `random_state=42` to make the partition reproducible. The training set contains 218 records and the test set contains 94 records. The split is performed before any preprocessing is fitted.

**Missing values.** The predictor variables contain one missing value in the 2018–2019 modeling subset. The target variable contains no missing values. The missing predictor value is handled by `SimpleImputer` with the median strategy, which replaces missing values with the median of the training data for the corresponding variable. The median was chosen because it is less sensitive to extreme values than the mean, which is relevant given the outliers preserved in Section 4.3.

**Scaling.** The predictors are standardized with `StandardScaler`, which transforms each variable to zero mean and unit variance using the statistics learned from the training set. Standardization places variables with different units and ranges (for example, GDP per capita and perception-based indicators) on a comparable scale.

**Preprocessing sequence.** Imputation and scaling are implemented using scikit-learn's `Pipeline` functionality. The preprocessing steps were fitted on the training set and applied to the test set using the learned parameters. The preprocessor is fitted on the training set only and then applied to the test set using the parameters learned from training. After preprocessing, no missing values remain, and the training and test matrices keep the same number of columns as the original predictor set, with 218 and 94 rows respectively.

The preprocessing steps do not include automatic feature selection, outlier removal, dimensionality reduction, or categorical encoding, because the predictors are all numerical and no such step was required by the selected variables.

### 5.3 Prevention of Target Leakage

Several measures were applied to prevent information from the target or from the test set from influencing the model:

- **Separation of target and predictors.** `happiness_score` is stored separately as `y` and is not part of the predictor matrix `X`. The notebook includes an assertion that verifies the target is not among the predictors.
- **Exclusion of the rank variable.** `happiness_rank` is excluded from the predictors. It is a position derived from the happiness score, so including it would give the model information that is directly tied to the value it is trying to predict.
- **Exclusion of non-predictive identifiers.** `country`, `year`, and `region` are not used as predictors, as described in Section 5.1.
- **Fitting on the training set only.** The imputation statistics (medians) and the scaling parameters (means and standard deviations) are estimated exclusively from the training records. The test records are only transformed using these parameters, so no information from the test set influences the preprocessing.
- **Split before preprocessing.** The partition into training and test sets is performed before any preprocessing object is fitted, which ensures that the evaluation data remains unseen during the entire preparation stage.
- **Structured validation.** The notebook includes assertions and printed checks to verify the predictor selection, train/test dimensions, missing values after preprocessing, and the fitting sequence of the preprocessing steps. These checks document the implemented workflow; printed statements describing design properties are not independent programmatic validations.

Model training, evaluation metrics, and the serialization of the final pipeline to `models/model.pkl` are described in Section 6.

---


## 6. Machine Learning Pipeline

### 6.1 Training and Evaluation of the Regression Model

**Status:** **Implemented** in `notebooks/model_training.ipynb`.

The regression model is a `LinearRegression` from scikit-learn, integrated into a single `Pipeline` with three sequential steps: `SimpleImputer(strategy="median")` for missing values, `StandardScaler()` for standardization, and `LinearRegression()` as the estimator. Packaging the preprocessing and the estimator together ensures that the same transformations learned during training are applied to any new data at prediction time.

The model is trained on the 2018 and 2019 records of the unified dataset, using the training partition of 218 observations obtained from the 70/30 split described in Section 5.2. The pipeline is fitted only on the training set, and the test partition of 94 observations is used exclusively to generate predictions and compute the evaluation metrics. Because the imputation and scaling are part of the pipeline, their parameters are estimated from the training data alone during `fit`.

The evaluation is based on a single train/test split with `random_state=42`. No cross-validation, hyperparameter tuning, or comparison against other models was performed in the implemented notebook. The results therefore describe the performance of this specific model on this specific partition.

### 6.2 Evaluation Metrics (MAE, RMSE, and R²)

**Status:** **Implemented.** Metrics were computed on the 94 test records.

| Metric | Value | Interpretation |
|---|---|---|
| MAE | 0.4124 | Mean absolute difference between predicted and actual happiness scores. |
| RMSE | 0.5207 | Square root of the mean squared error. |
| R² | 0.7651 | Proportion of the variability in the observed scores explained by the model on the test set. |

**Mean Absolute Error (MAE)** is the average of the absolute differences between predicted and observed scores. It is expressed in the same units as `happiness_score`, so an MAE of 0.4124 indicates that, on average, the predictions differ from the observed values by about 0.41 points on the report's scale.

**Root Mean Squared Error (RMSE)** is the square root of the average of the squared differences. Because the errors are squared before averaging, RMSE gives more weight to large deviations than MAE does. Its value (0.5207) being higher than the MAE indicates that the errors are not uniform, with some predictions deviating more than others.

**Coefficient of determination (R²)** measures the proportion of the variability of the observed test scores that is explained by the model. An R² of 0.7651 means that the model explains approximately 76.51% of the variability observed in the test set. This is a measure of explained variance, not a percentage of accuracy, and it is computed on a single test partition of 94 records, so it should be interpreted with that limitation in mind.

### 6.3 Pipeline Serialization (`model.pkl`)

**Status:** **Implemented and validated** in `notebooks/model_training.ipynb`.

The complete fitted pipeline, including the imputer, scaler, and linear regression estimator, is serialized using `joblib.dump()` and saved to `models/model.pkl`. Saving the entire pipeline preserves the preprocessing parameters learned during training, ensuring that new observations undergo the same transformations before prediction.

The saved artifact was successfully reloaded using `joblib.load()`. A post-reload functional validation was performed by generating predictions for five records from the test set.

The validation confirmed that:

- The model file exists at the expected path.
- The serialized pipeline can be successfully reloaded.
- The pipeline retains its three steps: `imputer`, `scaler`, and `regressor`.
- Five sample predictions were generated successfully.
- All generated predictions contain valid numerical values.
This verification confirms that the serialized pipeline is recoverable and functional for subsequent inference tasks.
---


Antes de entregar la sección 7, hay dos puntos que conviene aclarar porque afectan la exactitud del README:

1.  **Los totales finales (314 raw, 312 predicciones, 1 y 1 por estado)** los dejo como resultados esperados, no verificados, tal como indicas. Si ejecutaste la consulta SQL agregada, compártela o su salida y lo cambio a verificado.
2.  **La afirmación "consumer lag = 0 en las tres particiones"** y las salidas de las pruebas directas las tomo de tu descripción. No tengo el código ni la salida de esas ejecuciones, así que las presento como resultados reportados por el proyecto.

También elegí no incluir ningún código ni nombres de archivo más allá de los que proporcionaste, y no describo el sistema como de producción ni con garantías de exactly-once.


## 7. Kafka Pipeline and Event Contract

The streaming stage of the project simulates the continuous arrival of World Happiness records. A producer publishes historical records as events to Apache Kafka, and a consumer receives them, persists the original message, validates its structure and values, generates a prediction with the previously trained model, and stores the results in PostgreSQL. The pipeline is designed to demonstrate streaming ingestion and real-time inference over bounded historical data; it is not a production-grade distributed system.

The model uses six predictors (`gdp`, `social_support`, `health`, `freedom`, `generosity`, and `corruption`) to estimate the target `happiness_score`. The event carries an additional field, `actual_happiness_score`, which is used only for comparison and evaluation after prediction. It is never passed to the model as a feature.

Throughout this section, four stages of a record are distinguished:

| Stage | Description |
|---|---|
| **Original raw event** | The message as received from Kafka, stored unchanged in `raw_happiness_events` before any processing. |
| **Validated event** | An event that passed the event contract checks and is marked `VALID`. |
| **Model input** | The six-feature row built from the validated event and passed to the model. |
| **Prediction output** | The predicted score generated by the model, stored with the actual score and the prediction error. |

### 7.1 Event Contract Definition

The event contract is implemented in `streaming/event_contract.py`. It defines the fields every event must carry, their expected types, and the rules applied before an event can be used for inference.

| Field | Description |
|---|---|
| `event_id` | Unique identifier of the event, used for traceability. |
| `country` | Country associated with the observation. Also used as the Kafka message key. |
| `year` | Observation year. Only `2018` and `2019` are supported. |
| `gdp` | GDP-related predictor. |
| `family` | Social support predictor. Mapped to the model feature `social_support` (see below). |
| `health` | Health-related predictor. |
| `freedom` | Freedom-related predictor. |
| `generosity` | Generosity predictor. |
| `corruption` | Perceived corruption predictor. |
| `actual_happiness_score` | Observed happiness score, reserved for evaluation. |
| `event_time` | Event timestamp in ISO 8601 format. |

**Field mapping.** The event contract uses the field name `family` because it is the name specified by the workshop event format. The trained model, however, was trained with the feature `social_support`, which is the harmonized name used in the unified dataset (Section 4.2). The consumer therefore renames `family` to `social_support` before building the model input. The two names refer to the same predictor.

**Validation rules.** The validator checks:
- The presence of all required fields.
- The expected data type of each field.
- That `year` is one of the supported years, `2018` or `2019`.
- That numeric values are finite.
- That `event_time` is a valid ISO 8601 timestamp.

Predictor fields may be null. The trained pipeline includes a median imputer, so missing predictor values can be handled at inference time and do not, by themselves, invalidate an event.

**Validation statuses.** The validation function returns one of three statuses:
- `VALID`: the event satisfies the schema and value rules.
- `INVALID_SCHEMA`: the structure of the event is wrong. Required fields are missing or fields have unexpected types, so the message cannot be interpreted as an event.
- `INVALID_VALUES`: the structure is correct, but one or more values violate a business rule, such as an unsupported year or a non-finite number.

The distinction matters for diagnosis: a schema error indicates a problem with the producer or the message format, whereas a value error indicates a problem with the content of an otherwise well-formed event.

### 7.2 Kafka Topic Configuration

Events are published to the topic `happiness-events` with the following configuration:

| Parameter | Value |
|---|---|
| Topic name | `happiness-events` |
| Partitions | 3 |
| Replication factor | 1 |
| Message key | Country name |
| Bootstrap server | `localhost:9092` |

The replication factor of 1 means that each partition has a single copy on the local broker. The configuration is intended for a single-broker development environment and does not provide replication or high availability.

Using the country as the message key allows messages for the same country to be routed to the same partition, under Kafka's default key-based partitioning. As a result, the events of a given country maintain their relative order within that partition, which is the property the pipeline relies on when processing observations per country.

### 7.3 Producer: Data Flow Simulation

The producer is implemented in `streaming/producer.py`. Its purpose is to simulate the arrival of records over time from historical data. The source is the 2018 and 2019 records of the unified dataset, which corresponds to the 312 observations used for modeling. The data is not obtained from an external API or from live sources; the streaming behavior is simulated by replaying historical records one at a time.

For each source record, the producer:
1. Builds an event containing the contract fields listed in Section 7.1.
2. Serializes the event as JSON.
3. Publishes it to the topic `happiness-events`, using the country name as the message key.

Publishing one record at a time, rather than loading the whole file into Kafka at once, reproduces the arrival pattern of a stream. The producer publishes **312 events** in total, one for each 2018 and 2019 observation.

An example of the message structure, with illustrative values, is shown below:

```json
{
  "event_id": "E001",
  "country": "Colombia",
  "year": 2019,
  "gdp": 1.2,
  "family": 0.8,
  "health": 0.9,
  "freedom": 0.6,
  "generosity": 0.3,
  "corruption": 0.1,
  "actual_happiness_score": 6.2,
  "event_time": "2026-10-30T15:30:00"
}
```

### 7.4 Consumer: Real-Time Reception and Inference

The consumer is implemented in `streaming/consumer.py`. It loads the trained pipeline from `models/model.pkl` using `joblib`, subscribes to `happiness-events`, and processes messages one at a time.

**Configuration.** The final configuration uses:
- Consumer group `happiness-ml-team`.
- `enable_auto_commit=False`, so offsets are committed explicitly by the application.
- `auto_offset_reset="earliest"`, so a consumer group without committed offsets starts from the beginning of the topic.
- `max_poll_records=1`, so each poll returns a single message and the offset is committed after that message is processed.

**Processing sequence.** For each message, the consumer performs the following steps in order:

1. **Persist the original message.** The raw Kafka message is stored in `raw_happiness_events` before any validation or transformation. The stored record includes the original content and Kafka metadata, and it is initially marked for processing. This allows the pipeline to retain messages before validation, including records that are subsequently rejected, provided that the database insertion succeeds.
2. **Validate the event.** The event is checked with `validate_event()`. The result is `VALID`, `INVALID_SCHEMA`, or `INVALID_VALUES`. The raw record is updated with this status and, when applicable, the validation errors. Invalid events stop at this point and do not proceed to inference.
3. **Build the model input.** For valid events, `family` is renamed to `social_support`, and the six model features are placed in a one-row pandas DataFrame in the order used during training.
4. **Generate the prediction.** The loaded pipeline runs `model.predict()` on this row, producing the predicted happiness score. The actual score is not part of the input.
5. **Resolve dimension keys.** The country and date dimension identifiers are retrieved from `dim_country` and `dim_date`.
6. **Store the result.** The actual score, predicted score, and prediction error are saved in `fact_predictions`, linked to the originating raw event.
7. **Update the status and commit the offset.** The raw event's processing status is updated to reflect the outcome, and the Kafka offset is committed once the message has been processed.

When the consumer is stopped, it closes its connection to Kafka.

**Database tables.** The consumer writes to four PostgreSQL tables:
- `raw_happiness_events`: stores the original messages, Kafka metadata, processing status, and validation errors. It is the traceability layer of the pipeline.
- `dim_country`: the country dimension, used to link each prediction to its country.
- `dim_date`: the date dimension, used to link each prediction to its observation date.
- `fact_predictions`: stores the actual score, predicted score, and prediction error for each processed event. It is the analytical table used by the dashboard.

### 7.5 Event Validation and Invalid Record Handling

Invalid records are retained, not discarded. Because the raw event is persisted before validation, every message the consumer receives leaves a trace in `raw_happiness_events`, together with its status and validation errors. Only events with status `VALID` continue to inference, which means that invalid records never produce predictions.

**Direct validation tests.** The validation function was tested directly with three inputs:
- A valid event returned `VALID`.
- An event with missing required fields returned `INVALID_SCHEMA`.
- An event with the unsupported year `2020` returned `INVALID_VALUES`.

**Pipeline tests.** Two additional invalid events were published to Kafka and processed by a separate test consumer group, so that they did not interfere with the production consumer group:

| Test | Event ID | Description | Expected status | Observed status |
|---|---|---|---|---|
| Test 1 | `test-invalid-001` | Missing required fields | `INVALID_SCHEMA` | `INVALID_SCHEMA` |
| Test 2 | `test-invalid-002` | Complete structure, unsupported year `2020` | `INVALID_VALUES` | `INVALID_VALUES` |

Both records were stored in `raw_happiness_events` with their validation errors and did not generate predictions.

**Results of the regular run.** The 312 valid events published by the producer were processed as follows:
- Raw events stored: 312.
- Events with status `VALID`: 312.
- Predictions stored: 312.
- Consumer lag after processing: 0 across all three partitions.

These 312 events are the regular producer output. The two invalid test events are separate records and are not part of that count.

**Expected aggregate totals.** After the tests above, the database is expected to contain:
- Total raw events: 314 (312 valid plus 2 test records).
- Total predictions: 312.
- Records with `INVALID_SCHEMA`: 1.
- Records with `INVALID_VALUES`: 1.

These totals follow from the individual results above. They are expected values and have not been verified by a final aggregate query.

**Design considerations.** The pipeline relies on explicit offset commits after each message, so that an offset is advanced only once its message has been handled. This design is appropriate for a single-consumer academic environment, but it does not, by itself, guarantee exactly-once processing or protection against failures in every stage. The documented behavior covers the tested scenarios described above.



---

## 8. Database Schema

The analytical storage layer is implemented in PostgreSQL. The schema is defined in `sql/create_tables.sql`, the database operations are implemented in `streaming/db_operations.py`, and connections are obtained through `streaming/db_connection.py` using Psycopg 3. This section describes the declared schema, the implemented operations, and the results observed in the database `happiness_db`.

The schema comprises four tables: `raw_happiness_events` for the original Kafka messages, `dim_country` and `dim_date` as dimension tables, and `fact_predictions` for the analytical results.

### 8.1 Raw Storage (`raw_happiness_events`)

The table `raw_happiness_events` stores each Kafka message as received, together with the metadata needed to identify its origin and follow its processing. It is the traceability layer of the pipeline: It is designed to retain each Kafka message before validation, including messages that subsequently fail validation, provided that the database insertion succeeds.

| Column | Type and constraints | Purpose |
|---|---|---|
| `raw_event_id` | `BIGSERIAL PRIMARY KEY` | Internal identifier of the raw record. Referenced by `fact_predictions`. |
| `event_id` | `TEXT UNIQUE` | Identifier of the event, taken from the message. |
| `raw_message` | `TEXT NOT NULL` | Original Kafka message content. |
| `country` | `VARCHAR(150)` | Country extracted from the event. |
| `observation_year` | `INTEGER` | Historical observation year of the World Happiness Report. |
| `event_time` | `TIMESTAMPTZ` | Event timestamp. |
| `processing_status` | `VARCHAR(30) NOT NULL DEFAULT 'PROCESSING'` | Processing state of the record. |
| `validation_errors` | `JSONB` | Validation errors associated with the event, when applicable. |
| `kafka_topic` | `VARCHAR(150) NOT NULL` | Topic from which the message was consumed. |
| `kafka_partition` | `INTEGER NOT NULL` | Partition of origin. |
| `kafka_offset` | `BIGINT NOT NULL` | Offset of the message within its partition. |
| `received_at` | `TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP` | Time at which the record was stored. |

The `processing_status` column is restricted by a `CHECK` constraint to the values `PROCESSING`, `VALID`, `INVALID_SCHEMA`, `INVALID_VALUES`, and `PREDICTION_ERROR`. The table also enforces two uniqueness constraints: `event_id` is unique, and the combination `(kafka_topic, kafka_partition, kafka_offset)` is unique, which identifies each Kafka message exactly once within the topic.

Records are inserted with the initial status `PROCESSING` and their status is subsequently updated according to the validation and prediction outcome. The status `PREDICTION_ERROR` is part of the declared constraint set; its occurrence is not part of the observed results reported in Section 8.4.

### 8.2 Analytical Dimensional Model

The analytical model follows a star-schema organization, with `fact_predictions` at the center and two dimension tables describing the country and the date associated with each prediction.

**`dim_country`** stores the countries identified in the events.

| Column | Type and constraints |
|---|---|
| `country_id` | `BIGSERIAL PRIMARY KEY` |
| `country_name` | `VARCHAR(150) NOT NULL UNIQUE` |

**`dim_date`** stores the dates associated with the processing of the events. Its columns are `date_id` (`BIGSERIAL PRIMARY KEY`), `full_date` (`DATE NOT NULL UNIQUE`), `day`, `month`, `year`, and `quarter`.

An important distinction applies to `dim_date`. Its `full_date` represents the date on which the Kafka event was emitted, not the historical observation year of the World Happiness Report. The historical year is stored in `raw_happiness_events.observation_year` (2018 or 2019), and the emission date is stored in `dim_date`. The two fields must not be confused.

**`fact_predictions`** stores one row per prediction.

| Column | Type and constraints | Purpose |
|---|---|---|
| `prediction_id` | `BIGSERIAL PRIMARY KEY` | Identifier of the prediction. |
| `raw_event_id` | `BIGINT NOT NULL UNIQUE` | Link to the originating raw event. |
| `country_id` | `BIGINT NOT NULL` | Link to `dim_country`. |
| `date_id` | `BIGINT NOT NULL` | Link to `dim_date`. |
| `actual_score` | `DOUBLE PRECISION NOT NULL` | Observed happiness score. |
| `predicted_score` | `DOUBLE PRECISION NOT NULL` | Score generated by the model. |
| `prediction_error` | `DOUBLE PRECISION NOT NULL` | Difference between actual and predicted score. |
| `prediction_timestamp` | `TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP` | Time at which the prediction was stored. |

The table declares foreign keys to `raw_happiness_events(raw_event_id)`, `dim_country(country_id)`, and `dim_date(date_id)`.

### 8.3 Relationship Between Raw Events and Predictions

The relationship between the raw table and the fact table is one-to-one at most. The `UNIQUE` constraint on `fact_predictions.raw_event_id` guarantees that a raw event can be referenced by at most one prediction. The foreign key to `raw_happiness_events` ensures that every prediction points to an existing raw record, so each prediction can be traced back to the exact Kafka message that produced it.

The dimensions are referenced by foreign keys from the fact table. Each prediction is therefore associated with exactly one country and one emission date, through `country_id` and `date_id`.

The following table summarizes the relations:

| Relation | Cardinality | Enforced by |
|---|---|---|
| `raw_happiness_events` → `fact_predictions` | One raw event to at most one prediction | `UNIQUE (raw_event_id)` and foreign key |
| `dim_country` → `fact_predictions` | One country to many predictions | Foreign key on `country_id` |
| `dim_date` → `fact_predictions` | One date to many predictions | Foreign key on `date_id` |

The raw table is the source of traceability and is not a dimension. The fact table joins the raw record with the country and date dimensions, so that the analytical layer can be queried without reading the raw messages.

### 8.4 Result Storage and Traceability

**Indexing.** The schema declares 13 indexes, including those created implicitly by primary key and unique constraints. Four explicit indexes were added to support the most common access patterns:

| Index | Table and column | Purpose |
|---|---|---|
| `idx_raw_status` | `raw_happiness_events(processing_status)` | Retrieval of records by processing status. |
| `idx_raw_country` | `raw_happiness_events(country)` | Retrieval of records by country. |
| `idx_predictions_country` | `fact_predictions(country_id)` | Aggregation of predictions by country. |
| `idx_predictions_date` | `fact_predictions(date_id)` | Aggregation of predictions by date. |

The unique constraints also create indexes on `dim_country.country_name`, `dim_date.full_date`, `raw_happiness_events.event_id`, the composite key `(kafka_topic, kafka_partition, kafka_offset)`, and `fact_predictions.raw_event_id`. These support the uniqueness rules and the lookups performed during processing. The indexes were defined to match the access patterns of the pipeline; no performance measurements were carried out to quantify their effect.

**Write operations.** The operations are implemented in `streaming/db_operations.py`:
- `insert_raw_event()` inserts the original message and its Kafka metadata. It uses `ON CONFLICT DO NOTHING` on the combination of topic, partition, and offset. If the message already exists, the function retrieves its `raw_event_id`.
- `update_raw_event()` updates the extracted event metadata, the processing status, and the validation errors.
- `get_country_id()` inserts or retrieves the identifier of a country.
- `get_date_id()` inserts or retrieves the identifier of the emission date and derives day, month, year, and quarter.
- `save_prediction()` computes `prediction_error` as `actual_score - predicted_score` and stores the prediction, using `ON CONFLICT (raw_event_id) DO NOTHING`.

The `ON CONFLICT` clauses and the unique constraints make these insertions idempotent with respect to the keys they test: repeating an insertion for an existing message or prediction does not create a duplicate row. This is a local protection against duplication, not a global guarantee.

**Transactional scope.** Each function opens its own connection through `get_connection()`, so every operation runs in an independent transaction. The processing of a message is therefore not a single atomic transaction spanning all its writes, and the pipeline does not provide exactly-once processing. The safeguards described above act at the level of each individual insertion.

**Observed results.** The following results were obtained by querying the database `happiness_db`:

| Table | Records |
|---|---:|
| `raw_happiness_events` | 314 |
| `dim_country` | 160 |
| `dim_date` | 1 |
| `fact_predictions` | 312 |

The distribution of processing statuses in `raw_happiness_events` is:

| Processing status | Records |
|---|---:|
| `VALID` | 312 |
| `INVALID_SCHEMA` | 1 |
| `INVALID_VALUES` | 1 |

The traceability checks produced the following results:

| Metric | Result |
|---|---:|
| Total predictions | 312 |
| Unique raw events referenced | 312 |
| Countries referenced | 160 |
| Dates referenced | 1 |

Two additional checks were performed. No valid event lacks a prediction (`valid_without_prediction = 0`), and no invalid event has a prediction (`invalid_with_prediction = 0`). Every one of the 312 valid events has an associated prediction, and neither of the two invalid events has one.

**Emission date.** The distribution of emission dates among the valid events shows that all 312 were emitted on 2026-11-10. Accordingly, `dim_date` contains a single row:

| `date_id` | `full_date` | `day` | `month` | `year` | `quarter` |
|---:|---|---:|---:|---:|---:|
| 1 | 2026-11-10 | 10 | 11 | 2026 | 4 |

This single row is the expected result of a single-day run. It reflects the emission date of the Kafka events and does not indicate an error. The historical observation years of the records (2018 and 2019) are stored in `raw_happiness_events.observation_year`, not in `dim_date`.

---


## 9. Dashboard Explanation

The analytical layer of the project is implemented as a dashboard in Microsoft Power BI Desktop. The dashboard connects directly to the PostgreSQL database `happiness_db`, which stores the output of the streaming pipeline described in Sections 7 and 8. Its purpose is to support the exploration of the results: Power BI visualizes the predictions produced by the trained model and the observed happiness scores, but it does not generate predictions itself. All inference is performed by the consumer and the serialized pipeline (`models/model.pkl`).

The dashboard reads from four tables: `fact_predictions`, `dim_country`, `dim_date`, and `raw_happiness_events`. The Power BI data model reproduces the relationships declared in PostgreSQL:

| Relationship | Cardinality | Filter direction |
|---|---|---|
| `dim_country[country_id]` → `fact_predictions[country_id]` | One to many | Single, towards `fact_predictions` |
| `dim_date[date_id]` → `fact_predictions[date_id]` | One to many | Single, towards `fact_predictions` |
| `raw_happiness_events[raw_event_id]` → `fact_predictions[raw_event_id]` | One to one | Single, towards `fact_predictions` |

### 9.1 Key Performance Indicators (KPIs)

Five DAX measures were defined to summarize the model performance and the observed scores. The measures are calculated over `fact_predictions`, where each row corresponds to one prediction.

| Measure | Definition | What it measures |
|---|---|---|
| `Total Predictions` | `COUNTROWS('fact_predictions')` | Number of predictions stored. |
| `MAE` | Average of `ABS(prediction_error)` | Mean absolute difference between the observed and predicted scores. Lower values indicate a smaller average error. |
| `RMSE` | Square root of the average of `prediction_error²` | Root mean squared error. Larger errors contribute more strongly than in the MAE, so this measure is sensitive to large deviations. |
| `Average Actual Score` | `AVERAGE(actual_score)` | Mean observed happiness score of the selected records. |
| `Average Predicted Score` | `AVERAGE(predicted_score)` | Mean score generated by the model for the selected records. |

The measure `Total Predictions` returns 312, which matches the count verified in PostgreSQL. The values of `MAE`, `RMSE`, and the two averages depend on the filter context and are not reported in this section. The model-level metrics computed on the test partition are documented in Section 6.2 and should not be confused with the values calculated over the stored predictions in the dashboard.

Comparing the observed and predicted averages is useful because it shows whether the model reproduces the overall level of the scores, while the MAE and RMSE show how far individual predictions deviate from the observed values. A model may reproduce the average level well and still produce large errors for specific observations, which is why both kinds of measure are included.

### 9.2 Analytical Visualization

The visualizations are designed to analyze two aspects: the agreement between predicted and observed scores, and the distribution of observed scores across countries and years.

**Actual vs. predicted happiness scores.** A scatter plot compares `fact_predictions[actual_score]` on the horizontal axis with `fact_predictions[predicted_score]` on the vertical axis, with `raw_happiness_events[event_id]` as the detail field and `raw_happiness_events[observation_year]` as the legend. Points close to a diagonal reference line indicate predictions that agree with the observed scores. Points far from it indicate larger errors. The legend allows the two historical years, 2018 and 2019, to be distinguished.

**Mean absolute error by observation year.** A clustered column chart compares the `MAE` measure across the values of `raw_happiness_events[observation_year]`. It allows the error to be compared between 2018 and 2019, which is the only pair of years used for modeling (see Section 5.1).

**World happiness map.** A geographic visual places each country using `dim_country[country_name]`, with `Average Actual Score` as the value. Its purpose is to show the observed scores geographically and to compare countries. The map uses the geographic visual available in Power BI Desktop; the specific provider (Azure Maps or another) and the configuration of the color scale and tooltips have not been verified against the final file and should be confirmed before the README is published.

**Top 10 and bottom 10 countries.** Visuals ranking countries by `Average Actual Score` are **pending verification**. They are described here only as a planned analysis and should be documented once confirmed in the final dashboard.

**Filters.** The dashboard is designed to be segmented by `raw_happiness_events[observation_year]` and `dim_country[country_name]`. The presence of these slicers in the final version has not been verified and should be confirmed.

### 9.3 Database Connection and Results

The dashboard connects to PostgreSQL through the native PostgreSQL connector of Power BI Desktop. The connection uses the following parameters:

| Parameter | Value |
|---|---|
| Database server | `localhost:5433` |
| Database name | `happiness_db` |
| Database engine | PostgreSQL |
| Database user | `happiness_user` |
| Data source | PostgreSQL running in Docker |
| Connectivity mode | DirectQuery |

Authentication credentials are not documented in this README.

The connection uses **DirectQuery**. Each visual sends queries to the PostgreSQL database when it is rendered or filtered, so the dashboard always reflects the current contents of the tables instead of a static copy imported at a given moment. The dashboard does not use CSV files as a data source.

The results stored in the database were verified through SQL queries:

| Table | Records |
|---|---:|
| `raw_happiness_events` | 314 |
| `dim_country` | 160 |
| `dim_date` | 1 |
| `fact_predictions` | 312 |

Distribution of processing statuses in `raw_happiness_events`:

| Processing status | Records |
|---|---:|
| `VALID` | 312 |
| `INVALID_SCHEMA` | 1 |
| `INVALID_VALUES` | 1 |

All 312 predictions are associated with unique raw events, as described in Section 8.3.

**Temporal interpretation.** The records correspond to the historical observation years 2018 and 2019, which are stored in `raw_happiness_events[observation_year]` and are the field the dashboard uses to compare the two periods. The table `dim_date` contains a single date, `2026-11-10`, which is the date on which the Kafka events were emitted. It does not represent the historical year of the World Happiness Report. The two temporal dimensions should not be confused: the observation year describes the data, and the emission date describes the processing run.

---

## 10. Execution Instructions

This section describes how to reproduce the project from a clean clone of the repository. The instructions assume Windows with PowerShell and Docker Desktop. Commands that depend on repository files not reviewed for this section are marked as pending confirmation.

### 10.1 Prerequisites

The project is developed and executed on **Windows** using **PowerShell**. The following software must be installed on the host:

1. **Python** with a virtual environment named `.venv` in the project root. The Python version should be confirmed against the repository before being specified.
2. **Docker Desktop** configured to use **Linux containers**, including **Docker Compose**.
3. **Git**, to clone the repository and to keep local configuration files out of version control.

The following software runs inside Docker and does not need to be installed separately on the host:

- **Apache Kafka 4.0.0**, running in KRaft mode (without ZooKeeper).
- **PostgreSQL 16**.

The execution also requires the following project files, which must be present in the repository:

- `data/processed/happiness_unified.csv`: the unified dataset, containing 782 rows and 11 columns across the years 2015–2019.
- `models/model.pkl`: the serialized scikit-learn pipeline (imputer, scaler, and linear regression) used by the consumer for inference.

Python package dependencies are listed in `requirements.txt` (pending confirmation of the exact file name and contents).

### 10.2 Environment Configuration

**Step 1 – Navigate to the project directory.**

```powershell
cd C:\Users\USER\Desktop\ETL-Workshop-3
```

Replace the path with the location where the repository was cloned.

**Step 2 – Create and activate the virtual environment.**

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

If PowerShell blocks the activation script, it may be necessary to allow local scripts for the current session with `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass`.

**Step 3 – Install the Python dependencies.**

```powershell
pip install -r requirements.txt
```

**Step 4 – Create the local environment file.**

The database and Kafka settings are supplied through a local `.env` file, which is not versioned. Create it from the example file provided in the repository (pending confirmation that `.env.example` exists):

```powershell
Copy-Item .env.example .env
```

Then edit `.env` and assign values to the variables that the repository requires. The variables used in the project include `POSTGRES_DB`, `POSTGRES_USER`, and `POSTGRES_PASSWORD`. Any additional variables referenced by the Kafka and database modules must be confirmed before being listed here. Use the actual values for your local environment; do not commit them.

**Connection addresses.** The application uses different addresses depending on where it runs:

| Context | Kafka | PostgreSQL |
|---|---|---|
| Python scripts on the Windows host | `localhost:9092` | `localhost:5433` |
| Containers within the Docker network | `kafka:29092` | Service name within the Compose network (pending confirmation) |

**Files that must not be committed.** The following files and directories must remain excluded from Git, which should be verified in `.gitignore`:
- `.env`, because it contains credentials.
- `.venv/`, because it is local to each machine.

### 10.3 Infrastructure Deployment

**Step 1 – Validate the Compose configuration.**

```powershell
docker compose config
```

This command parses `docker-compose.yml` and reports errors before anything is started.

**Step 2 – Start the infrastructure.**

```powershell
docker compose up -d
```

The `-d` flag runs the containers in the background.

**Step 3 – Verify the container status.**

```powershell
docker compose ps
```

The Kafka and PostgreSQL containers should appear as running. During development they were named `happiness-kafka` and `happiness-postgres`; these names must be confirmed against the Compose file.

**Step 4 – Inspect the logs if necessary.**

```powershell
docker compose logs kafka
docker compose logs postgres
```

**Step 5 – Create the Kafka topic.**

The topic `happiness-events` was created during development with 3 partitions and a replication factor of 1:

```powershell
docker exec happiness-kafka /opt/kafka/bin/kafka-topics.sh --create --topic happiness-events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

Whether this step is automated by the Compose file or must be run manually has not been confirmed. Run it only if the topic does not already exist.

**Step 6 – Verify the topic configuration.**

```powershell
docker exec happiness-kafka /opt/kafka/bin/kafka-topics.sh --describe --topic happiness-events --bootstrap-server localhost:9092
```

The output should report 3 partitions and a replication factor of 1.

**Database schema initialization.** The table definitions are in `sql/create_tables.sql`. Whether this script runs automatically when the PostgreSQL container starts (for example, through an initialization directory mounted in the Compose file) or must be executed manually is **pending confirmation**. Until this is verified, the schema should be applied with the procedure used during development.

### 10.4 Producer and Consumer Execution

The producer and the consumer are Python processes that run on the host, connecting to Kafka at `localhost:9092` and to PostgreSQL at `localhost:5433`. The exact script paths, arguments, and startup order are **pending confirmation**. Based on the architecture described in Section 7, the expected order is:

1. Start the consumer first, so that it is subscribed to `happiness-events` when the events arrive.
2. Start the producer in a separate PowerShell terminal, with the virtual environment activated.

**Consumer.** The consumer subscribes to `happiness-events` using the consumer group `happiness-ml-team`. It loads `models/model.pkl`, persists each raw message to `raw_happiness_events` before validation, validates it against the event contract, generates a prediction for valid events, and stores the result in `fact_predictions`. Events that fail validation receive the status `INVALID_SCHEMA` or `INVALID_VALUES` and do not produce predictions. If the prediction step fails, the event is marked `PREDICTION_ERROR`. The Kafka offset is committed after each processing attempt. The implementation does not retry failed predictions automatically.

**Producer.** The producer reads the 2018 and 2019 records and publishes them as JSON events to `happiness-events`, using the country as the message key. It publishes 312 events in a normal run. The two invalid test events used during validation are not part of this workflow; they belong to a separate validation procedure described in Section 7.5.

Execution commands to be inserted once confirmed:

```powershell
# Terminal 1 – consumer
# python <consumer script path>

# Terminal 2 – producer
# python <producer script path>
```

### 10.5 Pipeline Functionality Verification

**Infrastructure checks.** Confirm that the Kafka and PostgreSQL containers are running with `docker compose ps`, and that the topic reports 3 partitions with `kafka-topics.sh --describe`, as shown in Section 10.3.

**Consumer group status.** The consumer group `happiness-ml-team` can be inspected to confirm that it has processed the messages and to check its lag:

```powershell
docker exec happiness-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group happiness-ml-team
```

A lag of 0 on all partitions indicates that the group has processed all messages available in the topic. This was the state observed after the reference execution.

**PostgreSQL checks.** The following queries use the tables and columns defined in `sql/create_tables.sql`, as documented in Section 8. They can be run through `psql` inside the PostgreSQL container, whose name and user are pending confirmation:

```sql
SELECT COUNT(*) FROM raw_happiness_events;
SELECT COUNT(*) FROM dim_country;
SELECT COUNT(*) FROM dim_date;
SELECT COUNT(*) FROM fact_predictions;

SELECT processing_status, COUNT(*)
FROM raw_happiness_events
GROUP BY processing_status
ORDER BY processing_status;

SELECT COUNT(DISTINCT raw_event_id) FROM fact_predictions;

SELECT COUNT(*) AS valid_without_prediction
FROM raw_happiness_events r
LEFT JOIN fact_predictions f ON f.raw_event_id = r.raw_event_id
WHERE r.processing_status = 'VALID' AND f.prediction_id IS NULL;

SELECT COUNT(*) AS invalid_with_prediction
FROM raw_happiness_events r
JOIN fact_predictions f ON f.raw_event_id = r.raw_event_id
WHERE r.processing_status IN ('INVALID_SCHEMA', 'INVALID_VALUES');
```

**Reference results.** During development, the following results were observed:

| Check | Result |
|---|---:|
| Records in `raw_happiness_events` | 314 |
| Records in `dim_country` | 160 |
| Records in `dim_date` | 1 |
| Records in `fact_predictions` | 312 |
| `VALID` records | 312 |
| `INVALID_SCHEMA` records | 1 |
| `INVALID_VALUES` records | 1 |
| Valid records without prediction | 0 |
| Invalid records with prediction | 0 |

These values are the historical results of the reference execution, which included the two invalid test events. They are not guaranteed for a new execution. The counts depend on the input data and on the database state, so a fresh run or a rerun may produce different totals.

**Stopping the infrastructure.** To stop the containers without removing their data, run:

```powershell
docker compose stop
```

To stop and remove the containers while preserving named volumes, run `docker compose down`. Removing volumes with `docker compose down -v` deletes the persisted data, including the PostgreSQL database, and should only be used when a complete reset is intended.

