#!/usr/bin/python3
### debug
# python3 -u sequential_processing.py ../input_configs/debug_seq.json
# python3 -u postfilter.py ../input_configs/debug_postfilter.json
# python3 -u tree_batch_cext.py ../input_configs/debug_tree_batch_cext.json
# python3 -u hybrid.py ../input_configs/debug_hybrid.json
### tree_batch_cext
# echo "1 ----------"
# python3 -u tree_batch_cext.py ../input_configs/tree_batch_cext_synthetic.json | tee ../results/tree_batch_cext_synthetic.print
# echo "2 ----------"
# python3 -u tree_batch_cext.py ../input_configs/tree_batch_cext_scale.json | tee ../results/tree_batch_cext_scale.print 
# echo "3 ----------"
# python3 -u tree_batch_cext.py ../input_configs/tree_batch_cext_job_light_single.json | tee ../results/tree_batch_cext_job_light_single.print
# echo "4 ----------"
# python3 -u tree_batch_cext.py ../input_configs/tree_batch_cext_job_light_join.json | tee ../results/tree_batch_cext_job_light_join.print 
# echo "5 ----------"
# python3 -u tree_batch_cext.py ../input_configs/tree_batch_cext_stats_ceb_single.json | tee ../results/tree_batch_cext_stats_ceb_single.print 
# echo "6 ----------"
# python3 -u tree_batch_cext.py ../input_configs/tree_batch_cext_stats_ceb_join.json | tee ../results/tree_batch_cext_stats_ceb_join.print 
# echo "7 ----------"
# python3 -u tree_batch_cext.py ../input_configs/tree_batch_cext_stats_ceb.json | tee ../results/tree_batch_cext_stats_ceb.print
# echo "8 ----------"
# python3 -u tree_batch_cext.py ../input_configs/tree_batch_cext_job_light.json | tee ../results/tree_batch_cext_job_light.print
# echo "9 ----------"
# python3 -u tree_batch_cext.py ../input_configs/tree_batch_cext_dsb_grasp_20k.json | tee ../results/tree_batch_cext_dsb_grasp_20k.print
# echo "10 ----------"
# python3 -u tree_batch_cext.py ../input_configs/tree_batch_cext_job_light_1k.json | tee ../results/tree_batch_cext_job_light_1k.print
# echo "11 ----------"
# python3 -u tree_batch_cext.py ../input_configs/tree_batch_cext_job_light_2k.json | tee ../results/tree_batch_cext_job_light_2k.print
# echo "12 ----------"
# python3 -u tree_batch_cext.py ../input_configs/tree_batch_cext_job_light_4k.json | tee ../results/tree_batch_cext_job_light_4k.print
# echo "13 ----------"
# python3 -u tree_batch_cext.py ../input_configs/tree_batch_cext_scale_batch10000.json | tee ../results/tree_batch_cext_scale_batch10000.print 
# echo "14 ----------"
# python3 -u tree_batch_cext.py ../input_configs/tree_batch_cext_scale_batch100000.json | tee ../results/tree_batch_cext_scale_batch100000.print 
# echo "15 ----------"
# python3 -u tree_batch_cext.py ../input_configs/tree_batch_cext_scale_batch1.json | tee ../results/tree_batch_cext_scale_batch1.print 
# echo "16 ----------"
# python3 -u tree_batch_cext.py ../input_configs/tree_batch_cext_job_light_batch5000.json | tee ../results/tree_batch_cext_job_light_batch5000.print 
# echo "17 ----------"
# python3 -u tree_batch_cext.py ../input_configs/tree_batch_cext_job_light_batch500000.json | tee ../results/tree_batch_cext_job_light_batch500000.print
### postfilter
# echo "1 ----------"
# python3 -u postfilter.py ../input_configs/postfilter_synthetic.json | tee ../results/postfilter_synthetic.print
# echo "2 ----------"
# python3 -u postfilter.py ../input_configs/postfilter_scale.json | tee ../results/postfilter_scale.print 
# echo "3 ----------"
# python3 -u postfilter.py ../input_configs/postfilter_job_light_single.json | tee ../results/postfilter_job_light_single.print
# echo "4 ----------"
# python3 -u postfilter.py ../input_configs/postfilter_job_light_join.json | tee ../results/postfilter_job_light_join.print 
# echo "5 ----------"
# python3 -u postfilter.py ../input_configs/postfilter_stats_ceb_single.json | tee ../results/postfilter_stats_ceb_single.print 
# echo "6 ----------"
# python3 -u postfilter.py ../input_configs/postfilter_stats_ceb_join.json | tee ../results/postfilter_stats_ceb_join.print 
# echo "7 ----------"
# python3 -u postfilter.py ../input_configs/postfilter_stats_ceb.json | tee ../results/postfilter_stats_ceb.print 
# echo "8 ----------"
# python3 -u postfilter.py ../input_configs/postfilter_dsb_grasp_20k.json | tee ../results/postfilter_dsb_grasp_20k.print
# echo "9 ----------"
# python3 -u postfilter.py ../input_configs/postfilter_job_light.json | tee ../results/postfilter_job_light.print
# echo "10 ----------"
# python3 -u postfilter.py ../input_configs/postfilter_job_light_1k.json | tee ../results/postfilter_job_light_1k.print
# echo "11 ----------"
# python3 -u postfilter.py ../input_configs/postfilter_job_light_2k.json | tee ../results/postfilter_job_light_2k.print
# echo "12 ----------"
# python3 -u postfilter.py ../input_configs/postfilter_job_light_4k.json | tee ../results/postfilter_job_light_4k.print
### sequential_processing
# echo "1 ----------"
# python3 -u sequential_processing.py ../input_configs/seq_synthetic.json
# echo "2 ----------"
# python3 -u sequential_processing.py ../input_configs/seq_scale.json
# echo "3 ----------"
# python3 -u sequential_processing.py ../input_configs/seq_job_light_single.json
# echo "4 ----------"
# python3 -u sequential_processing.py ../input_configs/seq_job_light_join.json
# echo "5 ----------"
# python3 -u sequential_processing.py ../input_configs/seq_stats_ceb_single.json 
# echo "6 ----------"
# python3 -u sequential_processing.py ../input_configs/seq_stats_ceb_join.json
# echo "7 ----------"
# python3 -u sequential_processing.py ../input_configs/seq_stats_ceb.json 
# echo "8 ----------"
# python3 -u sequential_processing.py ../input_configs/seq_dsb_grasp_20k.json 
# echo "9 ----------"
# python3 -u sequential_processing.py ../input_configs/seq_job_light.json 
# echo "10 ----------"
# python3 -u sequential_processing.py ../input_configs/seq_job_light_1k.json 
# echo "11 ----------"
# python3 -u sequential_processing.py ../input_configs/seq_job_light_2k.json 
# echo "12 ----------"
# python3 -u sequential_processing.py ../input_configs/seq_job_light_4k.json 
### hybrid
# echo "1 ----------"
# python3 -u hybrid.py ../input_configs/hybrid_synthetic.json | tee ../results/hybrid_synthetic.print
# echo "2 ----------"
# python3 -u hybrid.py ../input_configs/hybrid_scale.json | tee ../results/hybrid_scale.print 
# echo "3 ----------"
# python3 -u hybrid.py ../input_configs/hybrid_job_light_single.json | tee ../results/hybrid_job_light_single.print
# echo "4 ----------"
# python3 -u hybrid.py ../input_configs/hybrid_job_light_join.json | tee ../results/hybrid_job_light_join.print 
# echo "5 ----------"
# python3 -u hybrid.py ../input_configs/hybrid_stats_ceb_single.json | tee ../results/hybrid_stats_ceb_single.print 
# echo "6 ----------"
# python3 -u hybrid.py ../input_configs/hybrid_stats_ceb_join.json | tee ../results/hybrid_stats_ceb_join.print 
# echo "7 ----------"
# python3 -u hybrid.py ../input_configs/hybrid_stats_ceb.json | tee ../results/hybrid_stats_ceb.print
# echo "8 ----------"
# python3 -u hybrid.py ../input_configs/hybrid_dsb_grasp_20k.json | tee ../results/hybrid_dsb_grasp_20k.print
# echo "9 ----------"
# python3 -u hybrid.py ../input_configs/hybrid_job_light.json | tee ../results/hybrid_job_light.print