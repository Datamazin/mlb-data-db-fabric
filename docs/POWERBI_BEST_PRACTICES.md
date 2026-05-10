# Power BI Semantic Model Best Practices

## Summary

This document outlines official Microsoft best practices for Power BI semantic model naming conventions, based on Microsoft Learn documentation. These practices optimize models for Q&A, Copilot, and general usability.

**Key Principle**: Use **human-readable, descriptive names** that users would naturally say when asking questions about the data.

---

## Official Microsoft Guidance

### Primary Source
- **Microsoft Learn**: [Best practices to optimize Q&A in Power BI](https://learn.microsoft.com/power-bi/natural-language/q-and-a-best-practices)
- **Related**: [Copilot in Power BI: Prepare semantic model for AI](https://learn.microsoft.com/power-bi/create-reports/tutorial-copilot-power-bi-prepare-model)

### Core Recommendations

#### 1. Table Naming
- ✅ **Use simple, descriptive names** that reflect content
- ✅ **Avoid unnecessary words** like "Info", "List", "Summary" 
- ✅ **Use names users would say naturally**
- ❌ **Avoid abbreviations or jargon** that users wouldn't recognize

**Examples from Microsoft:**
- ✅ `Store` and `Products` (good)
- ❌ `StoreInfo` and `Product List` (unnecessarily verbose)
- ✅ `Customer` (good)
- ❌ `CustomerSummary` (forces unnatural phrasing)

#### 2. Column Naming
- ✅ **Descriptive and clear**
- ✅ **Spaces are acceptable** (and often preferred for readability)
- ✅ **Match how users talk about the data**
- ✅ **Split formatted columns** (e.g., "Full Address" → "Address", "City", "Country")

**Examples:**
- ✅ `Player ID`, `Team Name`, `Win Pct` (readable)
- ✅ `Snapshot Date`, `Games Back` (natural language)

#### 3. Consistency
- **Be consistent across the model** - mixing styles creates confusion
- All tables/columns should follow the same pattern
- Choose one naming convention and apply it uniformly

#### 4. Case Sensitivity
- **DAX is case-insensitive**: `SALES` and `Sales` are the same
- **Spaces are allowed**: `Home Team` is valid
- **No mandated casing**: Microsoft does not require PascalCase, camelCase, or any specific casing

---

## Common Misconceptions

### ❌ Myth: "Power BI requires PascalCase without spaces"
**Reality**: Microsoft documentation encourages **readable names with spaces** for Q&A/Copilot optimization.

### ❌ Myth: "Programming conventions apply to semantic models"
**Reality**: Semantic models prioritize **business user readability** over technical conventions. What works in code may not work for business users asking natural language questions.

---

## MLB Model Application

### Current State Analysis

**Consistent & Good:**
- `Player`, `Team`, `Venue`, `Game` - simple, clear
- `Player Season Summary`, `Home Team`, `Away Team` - descriptive with spaces

**Inconsistent:**
- `dim_season` - uses snake_case and technical prefix
- `head_to_head` - uses snake_case
- `Leaderboards` - mixed casing in columns (some camelCase like `playerId`, some Title Case like `Home Runs`)

### Recommended Changes

#### Tables to Rename (for consistency):
1. `dim_season` → `Season` 
   - Removes technical prefix "dim_"
   - Simplifies to natural language
   
2. `head_to_head` → `Head to Head`
   - Converts snake_case to readable format
   - Matches other table naming

#### Column Naming Standardization:

**Game table** - Standardize to readable format:
- `gameId` → `Game ID`
- `season` → `Season`
- `gameDate` → `Game Date`
- `gameDateTime` → `Game DateTime`
- `gameType` → `Game Type`
- `homeTeamId` → `Home Team ID`
- `awayTeamId` → `Away Team ID`
- `venueId` → `Venue ID`
- `loadedAt` → `Loaded At`

**Leaderboards table** - Fix inconsistencies:
- `playerId` → `Player ID`
- `fullName` → `Full Name`
- `teamId` → `Team ID`
- `season` → `Season`
- `gameType` → `Game Type`
- `games` → `Games`
- `pa` → `PA`
- `ab` → `AB`
- `hits` → `Hits`

**Apply same pattern across all tables:**
- IDs should be `[Entity] ID` (e.g., `Player ID`, `Team ID`)
- Dates should be `[Entity] Date` (e.g., `Game Date`, `Birth Date`)
- Keep acronyms uppercase when commonly known (e.g., `RBI`, `OBP`, `AVG`)

---

## Additional Best Practices

### Model Organization
1. **Add descriptions** to tables and columns
2. **Hide unnecessary columns** from report view
3. **Avoid duplicate field names** across tables
4. **Use display folders** to organize related fields

### Data Quality
1. **Set correct data types** (dates as Date, not String)
2. **Configure Data Category** for dates and geography
3. **Set Sort By Column** for logical ordering
4. **Configure Summarization** properly (ID columns should be "Don't Summarize")

### Synonyms
- Add **synonyms** for common variations
  - Example: "Customer" table could have synonyms: "Client", "Buyer", "Account"
  - "Revenue" column could have synonyms: "Sales", "Income", "Earnings"

### Relationships
- Ensure **all necessary relationships** are defined
- Mark **one relationship as active** between any two tables
- Consider denormalizing when multiple paths exist

---

## Implementation Notes

### For Copilot Optimization
Per [Microsoft's Copilot guidance](https://learn.microsoft.com/power-bi/create-reports/tutorial-copilot-power-bi-prepare-model):
- Use **human-readable names** for tables, columns, measures
- Provide **concise descriptions** (complete sentences)
- Distinguish between **similarly named fields** across tables
- Follow **star schema design** principles

### Testing
- Continuously test with Q&A/Copilot as you make changes
- Ask natural language questions users would actually ask
- Refine based on how well the AI interprets your model

---

## References

1. [Best practices to optimize Q&A in Power BI](https://learn.microsoft.com/power-bi/natural-language/q-and-a-best-practices)
2. [Copilot in Power BI: Prepare semantic model for AI](https://learn.microsoft.com/power-bi/create-reports/tutorial-copilot-power-bi-prepare-model)
3. [DAX syntax reference - Naming requirements](https://learn.microsoft.com/dax/dax-syntax-reference#naming-requirements)
4. [Power BI implementation planning: Workspaces at the tenant level](https://learn.microsoft.com/power-bi/guidance/powerbi-implementation-planning-workspaces-tenant-level-planning#workspace-naming-conventions)

---

## Version History

- **2026-05-09**: Initial documentation based on Microsoft Learn research
  - Clarified that PascalCase is NOT a Microsoft requirement
  - Established that readable names with spaces are preferred
  - Documented consistency as the primary goal
