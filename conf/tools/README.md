Tools
-------------

Test Repo
------------
testrepo.py test repo statistic generation

```bash
python testrepo.py -qe_cluster 172.23.105.177:8091 -repo_cluster 172.23.99.54:8091
```
Allows for the following query scenarios

Changes across all components:
    SELECT component, subcomponent,
           ARRAY_COUNT(`changed`) as t_changed,
           ARRAY_COUNT(`new`) as t_new,
           ARRAY_COUNT(`removed`) as t_removed
    FROM `history`
    WHERE ts == '2016-12-02'

See new tests for a single component
    SELECT `new`
    FROM `history`
    WHERE ts == '2016-12-02'
     AND component == '2i' AND subcomponent == 'array-indexing-forestdb'

See full change history of a component
    SELECT *
    FROM `history`
    WHERE ts == '2016-12-02'
     AND component == '2i' AND subcomponent == 'array-indexing-forestdb'
