## Complete TF CPC

**Author: Alex Matrunich, Marco Garieri, Bo Werth, Christian Mongeau**

**Description:**

The trade module is divided in two submodules: **complete\_tf\_cpc** and
**total\_trade\_CPC**. Each module is year specific. This means that, at the
time being, the trade module run indipendently for each year. In order to
run the **tt total\_trade\_CPC**, the output of **complete\_tf\_cpc** is
needed.






### Input Data

**Supplementary Datasets:**

- `hsfclmap2`: hs->fcl map (from mdb files)
- `adjustments`: old adjustment notes
- `unsdpartnersblocks`: UNSD area codes (M49)
- `fclunits`
- `comtradeunits`
- `EURconversionUSD`




#### UNSD TL Data

##### Chapters

The module downloads only records of commodities of interest for Tariffline
Data. The HS chapters are the following: 01, 02, 03, 04, 05, 06, 07, 08, 09,
10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 33, 35, 38, 40,
41, 42, 43, 50, 51, 52, 53. In the future, if other commotidy are of
interest for the division, it is important to include additional chapter in
the first step of the downloading. For Eurostat Data no filtering is
applied.



##### Transformation of TL Data

1) Remove duplicate values for which quantity & value & weight exist
(in the process, removing redundant columns). Note: missing quantity|weight
or value will be handled below by imputation



2) The tariffline data from UNSD contains multiple rows with identical
combination of reporter / partner / commodity / flow / year / qunit. Those
are separate registered transactions and the rows containinig non-missing
values and quantities are summed.



3) Remove non-numeric comm (hs) code; comm (hs) code has to be digit.
This probably should be part of the faoswsEnsure




**Treat unmapped ES Data**

Removing reporters for which we dont have mapping of commodities

- convert HS to FCL
- remove unmapped FCL codes
- join *fclunits*
- `NA` *fclunits* set to `mt`
- specific ES conversions: some FCL codes are reported in Eurostat
with different supplementary units than those reported in FAOSTAT









