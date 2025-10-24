# 🧩 Building Startup Analytics

[![Python](https://img.shields.io/badge/Python-3.11+-blue?logo=python)](https://github.com/PavelGrigoryevDS/olist-deep-dive/blob/main/pyproject.toml)
[![Web Report](https://img.shields.io/badge/📚_Web_Report-2563EB?logoColor=white)](https://pavelgrigoryevds.github.io/olist-deep-dive/)
[![Tableau](https://img.shields.io/badge/📊_Tableau_Dashboard-254E6B?logo=tableau&logoColor=white)](https://public.tableau.com/app/profile/pavel.grigoryev/viz/Inwork/PageSales)
[![Open In Kaggle](https://kaggle.com/static/images/open-in-kaggle.svg)](https://www.kaggle.com/code/pavelgrigoryev/deep-sales-analysis-eda-viz-rfm-nlp-geo)
[![Presentation](https://img.shields.io/badge/Slides-Google%20Slides-red)](https://docs.google.com/presentation/d/1sOYi3MWXedIEnuSn41H8lBeZ9aGnnTi5iV-DEMbfCvc/present)
[![Jupyter Notebook](https://img.shields.io/badge/-Notebook-F37626?logo=jupyter&logoColor=white)](https://github.com/PavelGrigoryevDS/olist-deep-dive/blob/main/olist_deep_dive/olist_deep_dive.ipynb)
[![MIT License](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)

Building analytics process for startup: infrastructure, dashboards, A/B testing, forecasting, automated reports, and anomaly detection.

<a id="contents"></a>

## 📑 Contents

- [🛠️ Tech Stack \& Methods](#tech-stack-methods)
- [📌 Project Overview](#project-overview)
- [🗃️ Data Source](#data-source)
- [🎯 Main Conclusions](#main-conclusions)
- [✨ Key Recommendations](#key-recommendations)
- [🚀 How to Run This Project](#how-to-run-this-project)

---

<a id="tech-stack-methods"></a>

## 🛠️ Tech Stack & Methods

**Stack:**

`Python` | `Pandas` | `ClickHouse` | `Superset` | `Yandex DataLens` | `Apache Airflow` | `Plotly` |  `StatsModels` | `SciPy` | `Pingouin` | `Telegram API` | `Uber Orbit`

**Methods:**

- **Data Infrastructure Design**: Star schema modeling, ETL pipeline development, and data quality validation
- **Product Analytics**: Retention analysis, cohort analysis, funnel analysis, and engagement metrics tracking
- **Business Intelligence**: Real-time dashboard design, KPI definition, and self-service reporting implementation
- **Statistical Hypothesis Testing**: A/A and A/B test analysis, sample size calculation, and statistical power analysis
- **Experiment Design**: Split system validation, metric sensitivity analysis, and multiple testing correction
- **Time Series Forecasting**: Bayesian structural models, trend/seasonality decomposition, and model validation
- **Anomaly Detection**: Statistical process control, moving average analysis, and alert threshold optimization
- **Automation Engineering**: DAG orchestration, API integration, and scheduled reporting systems
- **Causal Inference**: Difference-in-differences analysis for non-experimental data using CausalImpact
- **Monte Carlo Simulation**: Statistical power estimation and sample size determination through simulation
  
[⬆ back to top](#contents)

---

<a id="project-overview"></a>

## 📌 Project Overview

This project demonstrates the implementation of a complete product analytics system for an early-stage startup that has developed an innovative application merging a messenger with a personalized news feed. In this ecosystem, users can browse and interact with posts (views, likes) while simultaneously communicating with each other through direct messages. The core challenge was to build the entire analytical infrastructure from scratch to understand user behavior across both features and enable data-driven decision-making.

This initiative aimed to:

- **Establish Analytical Infrastructure**
  - Design and implement a scalable data architecture for real-time analytics
  - Create a single source of truth for product metrics and user behavior data

- **Enable Product Intelligence**
  - Build comprehensive dashboards for monitoring key product metrics
  - Analyze user retention patterns and engagement drivers
  - Investigate sudden metric fluctuations and user drop-off points

- **Drive Data-Driven Experimentation**
  - Implement rigorous A/B testing framework for feature validation
  - Ensure statistical reliability of experiment results through proper design

- **Develop Predictive Capabilities**
  - Create forecasting models for key business metrics
  - Enable proactive planning and early anomaly detection

- **Automate Business Intelligence**
  - Implement automated reporting systems for stakeholder updates
  - Build proactive alerting for metric anomalies and system issues

- **Generate Actionable Insights**
  - Translate data findings into concrete product recommendations
  - Establish continuous monitoring processes for business health

[⬆ back to top](#contents)

---

<a id="data-source"></a>

## 🗃️ Data Source

The analysis uses the **Olist Brazilian E-Commerce Dataset** ([Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)) 

[⬆ back to top](#contents)

## 📊 Detailed Case Study Walkthrough

### 1. Data Infrastructure

**Objective:** To create a reliable and performant analytics database to serve as the single source of truth for all downstream reporting and analysis.

**Implementation:**
- Designed and implemented a data model in ClickHouse optimized for analytical queries
- Wrote SQL scripts to create and populate fact and dimension tables
- Established ETL processes to ensure data consistency and freshness

**Key Deliverables & Insights:**
- [View the data model definition](./case_study/1_data_infrastructure/data_model_setup.sql)
- [Review data validation checks](./case_study/1_data_infrastructure/data_validation.ipynb)

**Business Impact:** Created a scalable foundation for all analytics, reducing data inconsistency issues and improving query performance by X%.

[↑ Back to Navigation](#-quick-navigation)

---

### 2. Product Dashboard

**Objective:** To provide the product and management teams with a real-time, self-service view of key product metrics (DAU, Retention, Engagement).

**Implementation:**
- Connected Superset to the ClickHouse data source
- Designed and built an intuitive dashboard with relevant visualizations
- Configured filters and interactive elements for granular exploration

**Key Deliverables & Insights:**
- ![Dashboard Screenshot](./case_study/2_product_dashboard/dashboard_screenshot.png)
- *Caption: The dashboard provides real-time visibility into core product health metrics*

**Business Impact:** Empowered non-technical teams to monitor product performance independently, reducing ad-hoc reporting requests by X%.

[↑ Back to Navigation](#-quick-navigation)

---

### 3. Metric Deep Dive

**Objective:** To investigate two critical business events: a surge of new users from a marketing campaign and a sudden drop in active audience.

**Implementation:**
- Analyzed retention curves for campaign-acquired users vs organic users
- Segmented users who experienced errors in the news feed
- Conducted cohort analysis to understand long-term user behavior

**Key Deliverables & Insights:**
- [Explore the full analysis notebook](./case_study/3_metric_deep_dive/retention_analysis.ipynb)
- **Finding 1:** Campaign users showed X% lower Day-7 retention compared to organic users
- **Finding 2:** The audience drop was correlated with [specific technical issue]

**Business Impact:** Provided actionable insights to improve marketing ROI and enabled engineering to quickly resolve a critical bug.

[↑ Back to Navigation](#-quick-navigation)

---

### 4. Experimentation & A/B Testing

**Objective:** To design, execute, and analyze an A/B test for a new feature, ensuring statistically sound results.

**Implementation:**
- Performed A/A test to validate the splitting system
- Calculated required sample size using power analysis
- Analyzed results using appropriate statistical methods (t-test, bootstrap)

**Key Deliverables & Insights:**
- [View experiment analysis](./case_study/4_ab_testing/experiment_analysis.ipynb)
- **Result:** The test showed a statistically significant X% improvement in [key metric]
- **Recommendation:** Launch feature to 100% of users

**Business Impact:** Data-driven feature rollout decision, expected to increase [business metric] by X%.

[↑ Back to Navigation](#-quick-navigation)

---

### 5. Metric Forecasting

**Objective:** To build a predictive model for key business metrics to support resource planning and goal setting.

**Implementation:**
- Utilized Uber's Orbit library for time series forecasting
- Trained and validated multiple models on historical data
- Compared model performance using appropriate metrics (MAPE, RMSE)

**Key Deliverables & Insights:**
- [Explore forecasting model](./case_study/5_metric_forecasting/forecast_model.ipynb)
- **Model Selected:** [Model name] achieved X% MAPE on validation data
- **Forecast:** Expected [metric] to grow by X% over next quarter

**Business Impact:** Enabled proactive business planning and early detection of potential metric deviations.

[↑ Back to Navigation](#-quick-navigation)

---

### 6. Automated Reporting

**Objective:** To automate daily metric reporting, ensuring stakeholders receive consistent updates without manual intervention.

**Implementation:**
- Built Airflow DAG for daily ETL pipeline
- Developed Python scripts to calculate key metrics
- Integrated Telegram API for automated report delivery

**Key Deliverables & Insights:**
- [View Airflow DAG code](./case_study/6_automated_reporting/daily_metrics_dag.py)
- [Telegram bot implementation](./case_study/6_automated_reporting/telegram_bot.py)
- *Screenshot of daily report in Telegram*

**Business Impact:** Saved X hours per week of manual reporting time and ensured consistent metric tracking.

[↑ Back to Navigation](#-quick-navigation)

---

### 7. Anomaly Detection

**Objective:** To implement a proactive system for detecting unusual metric behavior and alerting relevant teams.

**Implementation:**
- Created Airflow DAG running every 15 minutes
- Developed statistical methods for anomaly detection
- Built alerting system with contextual information

**Key Deliverables & Insights:**
- [View anomaly detection DAG](./case_study/7_anomaly_detection/anomaly_detection_dag.py)
- [Detection algorithm](./case_study/7_anomaly_detection/anomaly_logic.py)
- **Example:** System detected X anomalies in past week with Y% precision

**Business Impact:** Reduced mean time to detection for critical issues from X hours to Y minutes.

[↑ Back to Navigation](#-quick-navigation)

---

## 🎯 Key Business Impact Summary

| Area | Impact | Metric Improvement |
|------|--------|-------------------|
| **Decision Velocity** | Reduced time-to-insight from days to hours | X% faster decisions |
| **Operational Efficiency** | Automated manual reporting processes | X hours saved weekly |
| **Product Quality** | Proactive issue detection | X% faster bug resolution |
| **Revenue Impact** | Data-driven feature launches | X% uplift in key metrics |

---

## 🚀 How to Reproduce This Analysis

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/building-startup-analytics.git
   cd building-startup-analytics
```

[⬆ back to top](#contents)

## 📜 License  

This analysis is shared under [MIT License](LICENSE).  
Original data from Olist remains under its [Kaggle license](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).
