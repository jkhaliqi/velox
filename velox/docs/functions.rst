***********************
Presto Functions
***********************

.. toctree::
    :maxdepth: 1

    functions/presto/math
    functions/presto/decimal
    functions/presto/bitwise
    functions/presto/comparison
    functions/presto/string
    functions/presto/datetime
    functions/presto/array
    functions/presto/map
    functions/presto/regexp
    functions/presto/binary
    functions/presto/json
    functions/presto/conversion
    functions/presto/url
    functions/presto/aggregate
    functions/presto/window
    functions/presto/hyperloglog
    functions/presto/tdigest
    functions/presto/qdigest
    functions/presto/geospatial
    functions/presto/ipaddress
    functions/presto/uuid
    functions/presto/enum
    functions/presto/misc

Here is a list of all scalar and aggregate Presto functions available in Velox.
Function names link to function descriptions. Check out coverage maps
for :doc:`all <functions/presto/coverage>` and :doc:`most used
<functions/presto/most_used_coverage>` functions for broader context.

.. raw:: html

    <style>

    table.rows th {
        background-color: lightblue;
        border-style: solid solid solid solid;
        border-width: 1px 1px 1px 1px;
        border-color: #AAAAAA;
        text-align: center;
    }

    table.rows td {
        border-style: solid solid solid solid;
        border-width: 1px 1px 1px 1px;
        border-color: #AAAAAA;
    }

    table.rows tr {
        border-style: solid solid solid solid;
        border-width: 0px 0px 0px 0px;
        border-color: #AAAAAA;
    }

    table.rows td:nth-child(4), td:nth-child(6) {
        background-color: lightblue;
    }
    </style>

.. table::
    :widths: auto
    :class: rows

    =================================================  =================================================  =================================================  ==  =================================================  ==  =================================================
    Scalar Functions                                                                                                                                             Aggregate Functions                                    Window Functions
    =======================================================================================================================================================  ==  =================================================  ==  =================================================
    :func:`abs`                                        :func:`gte`                                        :func:`secure_rand`                                    :func:`any_value`                                      :func:`cume_dist`
    :func:`acos`                                       :func:`hamming_distance`                           :func:`secure_random`                                  :func:`approx_distinct`                                :func:`dense_rank`
    :func:`all_keys_match`                             :func:`hash_counts`                                :func:`sequence`                                       :func:`approx_most_frequent`                           :func:`first_value`
    :func:`all_match`                                  :func:`hmac_md5`                                   :func:`sha1`                                           :func:`approx_percentile`                              :func:`lag`
    :func:`any_keys_match`                             :func:`hmac_sha1`                                  :func:`sha256`                                         :func:`approx_set`                                     :func:`last_value`
    :func:`any_match`                                  :func:`hmac_sha256`                                :func:`sha512`                                         :func:`arbitrary`                                      :func:`lead`
    :func:`any_values_match`                           :func:`hmac_sha512`                                :func:`shuffle`                                        :func:`array_agg`                                      :func:`nth_value`
    :func:`array_average`                              :func:`hour`                                       :func:`sign`                                           :func:`avg`                                            :func:`ntile`
    :func:`array_constructor`                          :func:`infinity`                                   :func:`simplify_geometry`                              :func:`bitwise_and_agg`                                :func:`percent_rank`
    :func:`array_cum_sum`                              :func:`intersection_cardinality`                   :func:`sin`                                            :func:`bitwise_or_agg`                                 :func:`rank`
    :func:`array_distinct`                             :func:`inverse_beta_cdf`                           :func:`slice`                                          :func:`bitwise_xor_agg`                                :func:`row_number`
    :func:`array_duplicates`                           :func:`inverse_binomial_cdf`                       :func:`split`                                          :func:`bool_and`
    :func:`array_except`                               :func:`inverse_cauchy_cdf`                         :func:`split_part`                                     :func:`bool_or`
    :func:`array_frequency`                            :func:`inverse_chi_squared_cdf`                    :func:`split_to_map`                                   :func:`checksum`
    :func:`array_has_duplicates`                       :func:`inverse_f_cdf`                              :func:`split_to_multimap`                              :func:`classification_fall_out`
    :func:`array_intersect`                            :func:`inverse_gamma_cdf`                          :func:`spooky_hash_v2_32`                              :func:`classification_miss_rate`
    :func:`array_join`                                 :func:`inverse_laplace_cdf`                        :func:`spooky_hash_v2_64`                              :func:`classification_precision`
    :func:`array_max`                                  :func:`inverse_normal_cdf`                         :func:`sqrt`                                           :func:`classification_recall`
    :func:`array_max_by`                               :func:`inverse_poisson_cdf`                        :func:`st_area`                                        :func:`classification_thresholds`
    :func:`array_min`                                  :func:`inverse_t_cdf`                              :func:`st_asbinary`                                    :func:`convex_hull_agg`
    :func:`array_min_by`                               :func:`inverse_weibull_cdf`                        :func:`st_astext`                                      :func:`corr`
    :func:`array_normalize`                            :func:`ip_prefix`                                  :func:`st_boundary`                                    :func:`count`
    :func:`array_position`                             :func:`ip_prefix_collapse`                         :func:`st_buffer`                                      :func:`count_if`
    :func:`array_remove`                               :func:`ip_prefix_subnets`                          :func:`st_centroid`                                    :func:`covar_pop`
    :func:`array_sort`                                 :func:`ip_subnet_max`                              :func:`st_contains`                                    :func:`covar_samp`
    :func:`array_sort_desc`                            :func:`ip_subnet_min`                              :func:`st_convexhull`                                  :func:`entropy`
    :func:`array_subset`                               :func:`ip_subnet_range`                            :func:`st_coorddim`                                    :func:`every`
    :func:`array_sum`                                  :func:`is_finite`                                  :func:`st_crosses`                                     :func:`geometric_mean`
    :func:`array_sum_propagate_element_null`           :func:`is_infinite`                                :func:`st_difference`                                  :func:`geometry_union_agg`
    :func:`array_top_n`                                :func:`is_json_scalar`                             :func:`st_dimension`                                   :func:`histogram`
    :func:`array_union`                                :func:`is_nan`                                     :func:`st_disjoint`                                    :func:`khyperloglog_agg`
    :func:`arrays_overlap`                             :func:`is_null`                                    :func:`st_distance`                                    :func:`kurtosis`
    :func:`asin`                                       :func:`is_private_ip`                              :func:`st_endpoint`                                    :func:`make_set_digest`
    :func:`at_timezone`                                :func:`is_subnet_of`                               :func:`st_envelope`                                    :func:`map_agg`
    :func:`atan`                                       :func:`jaccard_index`                              :func:`st_envelopeaspts`                               :func:`map_union`
    :func:`atan2`                                      :func:`jarowinkler_similarity`                     :func:`st_equals`                                      :func:`map_union_sum`
    :func:`beta_cdf`                                   :func:`json_array_contains`                        :func:`st_exteriorring`                                :func:`max`
    :func:`between`                                    :func:`json_array_get`                             :func:`st_geometries`                                  :func:`max_by`
    :func:`bing_tile`                                  :func:`json_array_length`                          :func:`st_geometryfromtext`                            :func:`max_data_size_for_stats`
    :func:`bing_tile_at`                               :func:`json_extract`                               :func:`st_geometryn`                                   :func:`merge`
    :func:`bing_tile_children`                         :func:`json_extract_scalar`                        :func:`st_geometrytype`                                :func:`merge_set_digest`
    :func:`bing_tile_coordinates`                      :func:`json_format`                                :func:`st_geomfrombinary`                              :func:`min`
    :func:`bing_tile_parent`                           :func:`json_parse`                                 :func:`st_interiorringn`                               :func:`min_by`
    :func:`bing_tile_polygon`                          :func:`json_size`                                  :func:`st_interiorrings`                               :func:`multimap_agg`
    :func:`bing_tile_quadkey`                          :func:`laplace_cdf`                                :func:`st_intersection`                                :func:`noisy_approx_distinct_sfm`
    :func:`bing_tile_zoom_level`                       :func:`last_day_of_month`                          :func:`st_intersects`                                  :func:`noisy_approx_set_sfm`
    :func:`bing_tiles_around`                          :func:`least`                                      :func:`st_isclosed`                                    :func:`noisy_approx_set_sfm_from_index_and_zeros`
    :func:`binomial_cdf`                               :func:`length`                                     :func:`st_isempty`                                     :func:`noisy_avg_gaussian`
    :func:`bit_count`                                  :func:`levenshtein_distance`                       :func:`st_isring`                                      :func:`noisy_count_gaussian`
    :func:`bit_length`                                 :func:`like`                                       :func:`st_issimple`                                    :func:`noisy_count_if_gaussian`
    :func:`bitwise_and`                                :func:`line_interpolate_point`                     :func:`st_isvalid`                                     :func:`noisy_sum_gaussian`
    :func:`bitwise_arithmetic_shift_right`             :func:`line_locate_point`                          :func:`st_length`                                      :func:`numeric_histogram`
    :func:`bitwise_left_shift`                         :func:`ln`                                         :func:`st_linefromtext`                                :func:`qdigest_agg`
    :func:`bitwise_logical_shift_right`                :func:`localtime`                                  :func:`st_linestring`                                  :func:`reduce_agg`
    :func:`bitwise_not`                                :func:`log10`                                      :func:`st_multipoint`                                  :func:`regr_avgx`
    :func:`bitwise_or`                                 :func:`log2`                                       :func:`st_numgeometries`                               :func:`regr_avgy`
    :func:`bitwise_right_shift`                        :func:`longest_common_prefix`                      :func:`st_numinteriorring`                             :func:`regr_count`
    :func:`bitwise_right_shift_arithmetic`             :func:`lower`                                      :func:`st_numpoints`                                   :func:`regr_intercept`
    :func:`bitwise_shift_left`                         :func:`lpad`                                       :func:`st_overlaps`                                    :func:`regr_r2`
    :func:`bitwise_xor`                                :func:`lt`                                         :func:`st_point`                                       :func:`regr_slope`
    :func:`cardinality`                                :func:`lte`                                        :func:`st_pointn`                                      :func:`regr_sxx`
    :func:`cauchy_cdf`                                 :func:`ltrim`                                      :func:`st_points`                                      :func:`regr_sxy`
    :func:`cbrt`                                       :func:`map`                                        :func:`st_polygon`                                     :func:`regr_syy`
    :func:`ceil`                                       :func:`map_append`                                 :func:`st_relate`                                      :func:`reservoir_sample`
    :func:`ceiling`                                    :func:`map_concat`                                 :func:`st_startpoint`                                  :func:`set_agg`
    :func:`chi_squared_cdf`                            :func:`map_entries`                                :func:`st_symdifference`                               :func:`set_union`
    :func:`chr`                                        :func:`map_except`                                 :func:`st_touches`                                     :func:`skewness`
    :func:`clamp`                                      :func:`map_filter`                                 :func:`st_union`                                       :func:`stddev`
    :func:`codepoint`                                  :func:`map_from_entries`                           :func:`st_within`                                      :func:`stddev_pop`
    :func:`combinations`                               :func:`map_intersect`                              :func:`st_x`                                           :func:`stddev_samp`
    :func:`combine_hash_internal`                      :func:`map_key_exists`                             :func:`st_xmax`                                        :func:`sum`
    :func:`concat`                                     :func:`map_keys`                                   :func:`st_xmin`                                        :func:`sum_data_size_for_stats`
    :func:`construct_tdigest`                          :func:`map_keys_by_top_n_values`                   :func:`st_y`                                           :func:`tdigest_agg`
    :func:`contains`                                   :func:`map_keys_overlap`                           :func:`st_ymax`                                        :func:`var_pop`
    :func:`cos`                                        :func:`map_normalize`                              :func:`st_ymin`                                        :func:`var_samp`
    :func:`cosh`                                       :func:`map_remove_null_values`                     :func:`starts_with`                                    :func:`variance`
    :func:`cosine_similarity`                          :func:`map_subset`                                 :func:`strpos`
    :func:`crc32`                                      :func:`map_top_n`                                  :func:`strrpos`
    :func:`current_date`                               :func:`map_top_n_keys`                             :func:`subscript`
    :func:`current_timestamp`                          :func:`map_top_n_values`                           :func:`substr`
    :func:`current_timezone`                           :func:`map_values`                                 :func:`substring`
    :func:`date`                                       :func:`map_zip_with`                               :func:`t_cdf`
    :func:`date_add`                                   :func:`md5`                                        :func:`tan`
    :func:`date_diff`                                  :func:`merge_hll`                                  :func:`tanh`
    :func:`date_format`                                :func:`merge_khll`                                 :func:`timezone_hour`
    :func:`date_parse`                                 :func:`merge_sfm`                                  :func:`timezone_minute`
    :func:`date_trunc`                                 :func:`merge_tdigest`                              :func:`to_base`
    :func:`day`                                        :func:`millisecond`                                :func:`to_base64`
    :func:`day_of_month`                               :func:`minus`                                      :func:`to_base64url`
    :func:`day_of_week`                                :func:`minute`                                     :func:`to_big_endian_32`
    :func:`day_of_year`                                :func:`mod`                                        :func:`to_big_endian_64`
    :func:`degrees`                                    :func:`month`                                      :func:`to_geometry`
    :func:`destructure_tdigest`                        :func:`multimap_from_entries`                      :func:`to_hex`
    :func:`distinct_from`                              :func:`multiply`                                   :func:`to_ieee754_32`
    :func:`divide`                                     :func:`murmur3_x64_128`                            :func:`to_ieee754_64`
    :func:`dot_product`                                :func:`nan`                                        :func:`to_iso8601`
    :func:`dow`                                        :func:`negate`                                     :func:`to_milliseconds`
    :func:`doy`                                        :func:`neq`                                        :func:`to_spherical_geography`
    :func:`e`                                          :func:`ngrams`                                     :func:`to_unixtime`
    :func:`element_at`                                 :func:`no_keys_match`                              :func:`to_utf8`
    :func:`empty_approx_set`                           :func:`no_values_match`                            :func:`trail`
    :func:`ends_with`                                  :func:`noisy_empty_approx_set_sfm`                 :func:`transform`
    :func:`enum_key`                                   :func:`none_match`                                 :func:`transform_keys`
    :func:`eq`                                         :func:`normal_cdf`                                 :func:`transform_values`
    :func:`exp`                                        :func:`normalize`                                  :func:`trim`
    :func:`expand_envelope`                            :func:`now`                                        :func:`trim_array`
    :func:`f_cdf`                                      :func:`parse_datetime`                             :func:`trimmed_mean`
    :func:`fail`                                       :func:`parse_duration`                             :func:`truncate`
    :func:`filter`                                     :func:`parse_presto_data_size`                     :func:`typeof`
    :func:`find_first`                                 :func:`pi`                                         :func:`uniqueness_distribution`
    :func:`find_first_index`                           :func:`plus`                                       :func:`upper`
    :func:`flatten`                                    :func:`poisson_cdf`                                :func:`url_decode`
    :func:`flatten_geometry_collections`               :func:`pow`                                        :func:`url_encode`
    :func:`floor`                                      :func:`power`                                      :func:`url_extract_fragment`
    :func:`format_datetime`                            :func:`quantile_at_value`                          :func:`url_extract_host`
    :func:`from_base`                                  :func:`quantiles_at_values`                        :func:`url_extract_parameter`
    :func:`from_base32`                                :func:`quarter`                                    :func:`url_extract_path`
    :func:`from_base64`                                :func:`radians`                                    :func:`url_extract_port`
    :func:`from_base64url`                             :func:`rand`                                       :func:`url_extract_protocol`
    :func:`from_big_endian_32`                         :func:`random`                                     :func:`url_extract_query`
    :func:`from_big_endian_64`                         :func:`reduce`                                     :func:`uuid`
    :func:`from_hex`                                   :func:`regexp_extract`                             :func:`value_at_quantile`
    :func:`from_ieee754_32`                            :func:`regexp_extract_all`                         :func:`values_at_quantiles`
    :func:`from_ieee754_64`                            :func:`regexp_like`                                :func:`week`
    :func:`from_iso8601_date`                          :func:`regexp_replace`                             :func:`week_of_year`
    :func:`from_iso8601_timestamp`                     :func:`regexp_split`                               :func:`weibull_cdf`
    :func:`from_unixtime`                              :func:`reidentification_potential`                 :func:`width_bucket`
    :func:`from_utf8`                                  :func:`remap_keys`                                 :func:`wilson_interval_lower`
    :func:`gamma_cdf`                                  :func:`remove_nulls`                               :func:`wilson_interval_upper`
    :func:`geometry_as_geojson`                        :func:`repeat`                                     :func:`word_stem`
    :func:`geometry_from_geojson`                      :func:`replace`                                    :func:`xxhash64`
    :func:`geometry_invalid_reason`                    :func:`replace_first`                              :func:`xxhash64_internal`
    :func:`geometry_nearest_points`                    :func:`reverse`                                    :func:`year`
    :func:`geometry_to_bing_tiles`                     :func:`round`                                      :func:`year_of_week`
    :func:`geometry_to_dissolved_bing_tiles`           :func:`rpad`                                       :func:`yow`
    :func:`geometry_union`                             :func:`rtrim`                                      :func:`zip`
    :func:`great_circle_distance`                      :func:`scale_qdigest`                              :func:`zip_with`
    :func:`greatest`                                   :func:`scale_tdigest`
    :func:`gt`                                         :func:`second`
    =================================================  =================================================  =================================================  ==  =================================================  ==  =================================================
