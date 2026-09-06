# Data Dictionary

Describes all 12 columns in the dataset. Raw CSV contains intentional data quality issues;
Clean CSV has been processed by `etl/clean_data.py`.

---

| Column | Raw type | Clean type | Description | Possible values |
|---|---|---|---|---|
| `Transaction_ID` | string | string | Unique transaction identifier | `T-000000` … `T-019999` |
| `Transaction_Date` | string (mixed formats) | date (YYYY-MM-DD) | Date of transaction. Raw uses 4 formats: `%Y-%m-%d`, `%d/%m/%Y`, `%m-%d-%Y`, `%Y/%d/%m` | 2019-01-01 to 2025-12-31 |
| `Product_Category` | string (mixed case) | string (lowercase) | Hardware product category | `cpu`, `gpu`, `ram`, `ssd`, `hdd`, `monitor`, `mouse`, `keyboard`, `motherboard`, `case`, `fan`, `power supply`, `vga` |
| `Product_Name` | string | string (lowercase) | Full product name, generated as `{Brand} {Category} Model {number}` | e.g. `nvidia gpu model 512` |
| `Brand` | string (mixed case, ~2% null) | string (lowercase) | Hardware manufacturer | `intel`, `amd`, `nvidia`, `asus`, `msi`, `gigabyte`, `corsair`, `g.skill`, `logitech`, `razer`, `samsung`, `dell`, `hp` |
| `Unit_Price_USD` | string/float (dirty, ~40% null after clean) | float or null | Unit price in USD. Raw may contain `"199.99 USD"`, `"199.99 dollars"`, `"N/A"`, `""` | 10.00 – 2000.00 |
| `Quantity` | string/int (dirty, ~2% null) | int or null | Units sold. Raw may contain `"5 pcs"`, `"two"`, negative values | 1 – 10 |
| `Total_Amount_USD` | string/float (dirty) | float or null | Recalculated in ETL as `Unit_Price_USD × Quantity`. Raw value is discarded. Null when either price or quantity is null. | — |
| `Customer_Segment` | string | string (lowercase) | Type of customer | `retail`, `wholesale`, `online`, `enterprise`, `government` |
| `Region` | string | string (lowercase) | Geographic sales region | `north america`, `europe`, `asia pacific`, `vietnam`, `southeast asia`, `middle east` |
| `Payment_Method` | string (~2% null) | string (lowercase) | Payment channel used | `credit card`, `cash`, `bank transfer`, `e-wallet`, `cod` |
| `Is_Refund` | string (messy boolean, ~25% null) | bool or null | Whether the transaction was refunded. Raw uses mixed representations: `"True"`, `"1"`, `"yes"`, `"False"`, `"0"`, `"no"`, `""`, `None` | `True`, `False`, `null` |

---

## Null Rates (approximate, clean CSV)

| Column | Null rate | Reason |
|---|---|---|
| `Unit_Price_USD` | ~40% | Raw used `"N/A"`, `""`, or unparseable strings |
| `Total_Amount_USD` | ~41% | Derived — null whenever price or quantity is null |
| `Is_Refund` | ~25% | Raw used `""` or `None` for ~25% of rows |
| `Brand` | ~2% | Randomly set to `None` in generator |
| `Quantity` | ~2% | Negative values discarded; `"two"` unparseable |
| `Payment_Method` | ~2% | Randomly set to `None` in generator |
| `Transaction_Date` | 0% | All dates parsed after multi-format support added |
