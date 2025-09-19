DONE:

We now have the visualise-cluster/ endpoint, which is comparable to the is_cluster_valid.py one
hmpps_person_match/routes/cluster/is_cluster_valid.py

TODO:

The async def get_clusters function in
hmpps_cpr_splink/cpr_splink/interface/score.py

Does something very similar to what we want.

Next:
- See if we can re-use it
- If not, create a new similar function.  interface/ should be responsible for grabbing the data, but let's have functions in hmpps_cpr_splink/cpr_splink/visualisation/ that munge the data into the format required by the spec.



