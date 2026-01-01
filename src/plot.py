import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import json
from mpl_toolkits.axes_grid1.inset_locator import inset_axes, mark_inset
from matplotlib.gridspec import GridSpec

colors = {
    "seq": "#e89da0",
    "tree_batch_cext": "#88cee6",
    "postfilter": "#b2d3a4",
    "hybrid": "#b7a3cb",
}

alg_to_formal_name = {
    "seq": "IndProc",
    "tree_batch_cext": "BaCon",
    "postfilter": "PostFilt",
    "hybrid": "Hybrid",
}

datasets = [
    "synthetic",
    "scale",
    "job_light",
    "job_light_single",
    "stats_ceb",
    "stats_ceb_single",
    "stats_ceb_join",
    "dsb_grasp_20k",
]


result_dir = "../results/"


def load_aggregated_results():
    res = {}
    for dataset in datasets:
        res[dataset] = pd.read_csv(result_dir + dataset + ".tsv", sep="\t")
    return res


def plot_ordered_runtime():
    res = load_aggregated_results()

    alg_to_overheads = {}
    for alg in [
        "seq",
        "postfilter",
        "tree_batch_cext",
    ]:
        alg_to_overheads[alg] = []
        for dataset in datasets:
            _df = res[dataset]
            mask = _df["pattern_id"].astype(str).str.fullmatch(r"\d+")
            df = _df[mask].reset_index(drop=True)
            num_rows = len(df)

            for i in range(num_rows):
                _overhead = df.iloc[i]["sec_" + alg]
                if _overhead == "None":
                    _overhead = 0.0
                else:
                    _overhead = float(_overhead)
                if alg != "tree_batch_cext" and alg != "hybrid":
                    _overhead += float(df.iloc[i]["additional_at_least_" + alg])
                alg_to_overheads[alg].append(_overhead)
        alg_to_overheads[alg] = np.array(alg_to_overheads[alg])

    order_by = "seq"

    sort_idx = np.argsort(alg_to_overheads[order_by])
    N = sort_idx.shape[0]

    plt.clf()
    fig, ax = plt.subplots(figsize=(13.6, 3.0))
    for alg in [
        "seq",
        "tree_batch_cext",
        "postfilter",
    ]:
        cur_sorted = alg_to_overheads[alg][sort_idx]
        ax.plot(
            range(1, N + 1),
            cur_sorted,
            label=alg_to_formal_name[alg],
            linewidth=2.5,
            color=colors[alg],
        )

    print("!!!", alg_to_overheads["seq"][sort_idx].tolist()[120:130])

    diff = (
        alg_to_overheads["tree_batch_cext"][sort_idx]
        - alg_to_overheads["seq"][sort_idx]
    )
    largest_i, largest_gap = None, None
    for i in range(diff.shape[0]):
        if largest_i is None or largest_gap < diff[i]:
            largest_i, largest_gap = i + 1, diff[i]
    print(
        f"The largest gap happens at index {largest_i} with gap {largest_gap} seconds"
    )

    ax.set_xlabel(
        f"Join Pattern index (sorted by {alg_to_formal_name[order_by]} runtime)",
        fontsize=10,
    )
    ax.set_ylabel("Runtime (sec.)", fontsize=10)
    ax.set_yticks([1800, 3600, 5400, 7200])
    ax.grid(True, linestyle=":", linewidth=1.5, alpha=0.6)
    ax.tick_params(labelsize=10)
    ax.set_title("Runtime per Join Pattern", fontsize=11)
    ax.legend(fontsize=11)

    axins = inset_axes(
        ax,
        width="60%",  # % of parent axis
        height="35%",
        loc="upper center",
        # borderpad=3.0,
    )

    zoom_x_max = 200
    for alg in [
        "seq",
        "tree_batch_cext",
        "postfilter",
    ]:
        cur_sorted = alg_to_overheads[alg][sort_idx]
        axins.plot(
            range(1, zoom_x_max + 1),
            cur_sorted[:zoom_x_max],
            label=alg_to_formal_name[alg],
            linewidth=1.6,
            color=colors[alg],
        )

    axins.set_xlim(0, zoom_x_max)
    axins.set_ylim(0, 250)
    axins.grid(True, linestyle=":", linewidth=0.6, alpha=0.6)
    axins.set_yticks([10, 50, 100, 150, 200, 250])
    axins.tick_params(labelsize=8)

    plt.tight_layout()
    plt.savefig("../figures/ordered_runtime.png", dpi=300)


# def plot_cumulative_runtime():
#     res = load_aggregated_results()

#     alg_to_overheads = {}
#     for alg in [
#         "seq",
#         "postfilter",
#         "tree_batch_cext",
#     ]:
#         alg_to_overheads[alg] = []
#         for dataset in datasets:
#             _df = res[dataset]
#             mask = _df["pattern_id"].astype(str).str.fullmatch(r"\d+")
#             df = _df[mask].reset_index(drop=True)
#             num_rows = len(df)

#             for i in range(num_rows):
#                 _overhead = df.iloc[i]["sec_" + alg]
#                 if _overhead == "None":
#                     _overhead = 0.0
#                 else:
#                     _overhead = float(_overhead)
#                 if alg != "tree_batch_cext" and alg != "hybrid":
#                     _overhead += float(df.iloc[i]["additional_at_least_" + alg])
#                 alg_to_overheads[alg].append(_overhead)
#         alg_to_overheads[alg] = np.array(alg_to_overheads[alg])

#     order_by = "seq"

#     sort_idx = np.argsort(alg_to_overheads[order_by])
#     N = sort_idx.shape[0]

#     plt.clf()
#     fig, ax = plt.subplots(figsize=(6.8, 3.3))
#     for alg in [
#         "seq",
#         "tree_batch_cext",
#         "postfilter",
#     ]:
#         cur_sorted = alg_to_overheads[alg][sort_idx]
#         cur_cum = np.cumsum(cur_sorted)
#         ax.plot(
#             range(1, N + 1),
#             cur_cum,
#             label=alg_to_formal_name[alg],
#             linewidth=2,
#             color=colors[alg],
#         )

#     max_i, max_v, y_lim, lst_worse = None, None, None, None
#     tree_batch_cext_cum_sum = np.cumsum(alg_to_overheads["tree_batch_cext"][sort_idx])
#     seq_cum_sum = np.cumsum(alg_to_overheads["seq"][sort_idx])
#     for i in range(seq_cum_sum.shape[0]):
#         _diff = tree_batch_cext_cum_sum[i] - seq_cum_sum[i]
#         if max_v is None or max_v < _diff:
#             max_i = i + 1
#             max_v = _diff
#         if tree_batch_cext_cum_sum[i] > seq_cum_sum[i]:
#             lst_worse = i + 1
#             y_lim = tree_batch_cext_cum_sum[i]
#     print(
#         f"Maximum difference between tree_batch_cext against seq is at index {max_i} with difference {max_v}"
#     )
#     print(
#         f"The last index that tree_batch_cext's cumulative computational overhead is worse than seq is at index {lst_worse}"
#     )

#     ax.set_xlabel(
#         f"Join Pattern index (sorted by {alg_to_formal_name[order_by]} computational overhead)",
#         fontsize=9,
#     )
#     ax.set_ylabel("Cumulative computational overhead (sec.)", fontsize=9)
#     ax.tick_params(labelsize=10)
#     ax.set_title("Accumulated Computational Overhead per Join Pattern", fontsize=11)
#     ax.legend(fontsize=11)

#     axins = inset_axes(
#         ax,
#         width="30%",  # % of parent axis
#         height="30%",
#         loc="center left",
#         borderpad=2.2,
#     )

#     zoom_x_max = 150
#     for alg in [
#         "seq",
#         "tree_batch_cext",
#         "postfilter",
#     ]:
#         cur_sorted = alg_to_overheads[alg][sort_idx]
#         cur_cum = np.cumsum(cur_sorted)
#         axins.plot(
#             range(1, zoom_x_max + 1),
#             cur_cum[:zoom_x_max],
#             label=alg_to_formal_name[alg],
#             linewidth=1,
#             color=colors[alg],
#         )

#     print(alg_to_overheads["seq"][sort_idx][136])
#     print(alg_to_overheads["tree_batch_cext"][sort_idx][136])

#     axins.axvline(
#         x=max_i,
#         color="black",
#         linestyle=":",
#         linewidth=0.8,
#         alpha=0.5,
#     )
#     axins.axvline(
#         x=lst_worse,
#         color="black",
#         linestyle=":",
#         linewidth=0.8,
#         alpha=0.5,
#     )
#     axins.set_xlim(0, zoom_x_max)
#     axins.set_ylim(0, y_lim + 20)
#     axins.set_yticks([100, 314])
#     axins.set_xticks([1, 60, max_i, lst_worse])
#     axins.tick_params(labelsize=7)

#     plt.tight_layout()
#     plt.savefig("../figures/cumulative_runtime.png", dpi=300)


def plot_per_workload_barplot():
    res = load_aggregated_results()

    for dataset in ["scale"]:
        alg_to_overheads = {}
        alg_to_additional = {}
        for alg in [
            "seq",
            "postfilter",
            "tree_batch_cext",
        ]:
            alg_to_overheads[alg] = []
            alg_to_additional[alg] = []
            _df = res[dataset]
            mask = _df["pattern_id"].astype(str).str.fullmatch(r"\d+")
            df = _df[mask].reset_index(drop=True)
            num_rows = len(df)

            for i in range(num_rows):
                _overhead = df.iloc[i]["sec_" + alg]
                if _overhead == "None":
                    _overhead = 0.0
                else:
                    _overhead = float(_overhead)
                alg_to_overheads[alg].append(_overhead)
                if alg != "tree_batch_cext" and alg != "hybrid":
                    alg_to_additional[alg].append(
                        float(df.iloc[i]["additional_at_least_" + alg])
                    )
                else:
                    alg_to_additional[alg].append(0.0)
            alg_to_overheads[alg] = np.array(alg_to_overheads[alg])
            alg_to_additional[alg] = np.array(alg_to_additional[alg])

        mask = res[dataset]["pattern_id"].astype(str).str.fullmatch(r"\d+")
        df = res[dataset][mask].reset_index(drop=True)
        sort_idx = np.argsort(df["n_tables"])
        n_tables_per_pattern = df["n_tables"][sort_idx].reset_index(drop=True)

        N = alg_to_overheads["seq"].shape[0]

        width_per_bar = 0.5
        fig, ax = plt.subplots(figsize=(N * width_per_bar, 3.3))

        spacing = 2.0
        x = np.arange(N) * spacing
        bar_width = 0.4
        offsets = [-bar_width, 0, bar_width]

        for i, alg in enumerate(
            [
                "seq",
                "postfilter",
                "tree_batch_cext",
            ]
        ):
            ax.bar(
                x + offsets[i],
                alg_to_overheads[alg][sort_idx],
                bar_width,
                color=colors[alg],
                edgecolor="black",
                linewidth=0.5,
                label=f"{alg_to_formal_name[alg]}: runtime of avail. Qs",
            )

            ax.bar(
                x + offsets[i],
                alg_to_additional[alg][sort_idx],
                bar_width,
                bottom=alg_to_overheads[alg][sort_idx],
                color=colors[alg],
                edgecolor="black",
                linewidth=0.5,
                hatch="//",
                label=f"{alg_to_formal_name[alg]}: min. runtime of unavail. Qs",
            )

        _diff = (
            alg_to_overheads["tree_batch_cext"][sort_idx]
            - alg_to_overheads["seq"][sort_idx]
            - alg_to_additional["seq"][sort_idx]
        )
        top3_idx = np.argsort(_diff)[-3:][::-1]

        print("Top 3 largest (tree_batch_cext - seq):")
        for _i in top3_idx:
            print(
                f"\tpattern_id={_i}, original_pattern_id={sort_idx[_i]}, diff={_diff[_i]}"
            )

        _diff = (
            alg_to_overheads["tree_batch_cext"][sort_idx]
            - alg_to_overheads["postfilter"][sort_idx]
            - alg_to_additional["postfilter"][sort_idx]
        )
        top3_idx = np.argsort(_diff)[-3:][::-1]

        print("Top 3 largest (tree_batch_cext - postfilter):")
        for _i in top3_idx:
            print(
                f"\tpattern_id={_i}, original_pattern_id={sort_idx[_i]}, diff={_diff[_i]}"
            )

        lst_i = 0
        xx = []
        xx_labels = []
        for i in range(1, n_tables_per_pattern.shape[0]):
            if n_tables_per_pattern[i] != n_tables_per_pattern[i - 1]:
                xx.append(x[(lst_i + i) // 2])
                xx_labels.append(f"{n_tables_per_pattern[i - 1]}-table JPs")
                ax.axvline(
                    x=(x[i] + x[i - 1]) * 0.5,
                    color="black",
                    linestyle="-",
                    linewidth=0.8,
                )
                lst_i = i
        xx.append(x[(lst_i + n_tables_per_pattern.shape[0] - 1) // 2])
        xx_labels.append(
            f"{n_tables_per_pattern[n_tables_per_pattern.shape[0] - 1]}-table JPs"
        )

        ax_top = ax.secondary_xaxis("top")
        ax_top.tick_params(axis="x", length=0, width=0)
        ax_top.set_xticks(xx)
        ax_top.set_xticklabels(xx_labels)

        ax.set_ylabel("LOG Runtime", fontsize=11)
        ax.set_yscale("log")
        ax.set_xlabel(
            "Join Pattern Index (sorted by number of tables involved)", fontsize=11
        )

        ax.set_xlim(-1, (N - 1) * spacing + 1)
        ax.tick_params(axis="y", labelsize=10)
        tick_idx = np.arange(0, N, 5)
        ax.set_xticks(x[tick_idx])
        ax.set_xticklabels(tick_idx, fontsize=10)

        ax.grid(True, axis="y", linestyle=":", linewidth=0.6, alpha=0.6)

        ax.legend()

        plt.tight_layout(pad=0.4)
        plt.savefig(f"../figures/{dataset}_bar_per_join_pattern.png", dpi=300)


# plot the feature importance
def plot_model_importance(importance_file):
    with open(importance_file, "r") as fin:
        importance_dict = json.load(fin)

        features = list(importance_dict.keys())
        importances = np.array(list(importance_dict.values()), dtype=float)
        indices = np.argsort(importances)
        timestamp_str = (
            importance_file.split("/")[-1].split(".")[0].rstrip("_importance")
        )

        sorted_importances = importances[indices]
        sorted_features = [features[i] for i in indices]

        num_features = len(sorted_features)

        fig, ax = plt.subplots(figsize=(3.4, 1.7))
        ax.barh(
            range(num_features),
            sorted_importances,
            color="0.4",  # neutral gray
            edgecolor="black",
            linewidth=0.4,
        )

        ax.set_yticks(range(num_features))
        ax.set_yticklabels(sorted_features, fontsize=4.5)

        ax.set_xlabel("Feature importance", fontsize=3)

        ax.tick_params(axis="x", labelsize=3)
        ax.tick_params(axis="y", length=0)

        ax.grid(True, axis="x", linestyle=":", linewidth=0.6, alpha=0.6)

        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.spines["left"].set_visible(False)

        plt.tight_layout(pad=0.3)
        plt.savefig(
            f"../figures/{timestamp_str}_feature_importance.png",
            dpi=300,
            bbox_inches="tight",
        )


def plot_hybrid_per_pattern_barplot():
    res = load_aggregated_results()

    dataset_to_alg_to_overheads = {}
    dataset_to_alg_to_additional = {}
    dataset_to_pattern_id = {}
    dataset_to_formal_name = {"scale": "scale.sql", "stats_ceb": "stats-ceb.sql"}

    for dataset in ["scale", "stats_ceb"]:
        _df = res[dataset]
        mask_all = _df["pattern_id"].astype(str).str.fullmatch(r"\d+")
        df_all = _df[mask_all].reset_index()

        sort_idx = np.argsort(df_all["n_tables"])

        mask_hybrid = df_all["switch_to_hybrid"].isin(["PostFilter", "SeqProc"])
        df_sub = df_all[mask_hybrid].reset_index(drop=True)

        # get the idx (ordered by n_tables) for the subset that switches to baseline
        dataset_to_pattern_id[dataset] = sort_idx[mask_hybrid[sort_idx]].index.tolist()

        num_rows = len(df_sub)

        dataset_to_alg_to_overheads[dataset] = {}
        dataset_to_alg_to_additional[dataset] = {}
        for alg in [
            "seq",
            "postfilter",
            "tree_batch_cext",
            "hybrid",
        ]:
            dataset_to_alg_to_overheads[dataset][alg] = []
            dataset_to_alg_to_additional[dataset][alg] = []

            for i in range(num_rows):
                _overhead = df_sub.iloc[i]["sec_" + alg]
                if _overhead == "None":
                    _overhead = 0.0
                else:
                    _overhead = float(_overhead)
                dataset_to_alg_to_overheads[dataset][alg].append(_overhead)
                if alg != "tree_batch_cext" and alg != "hybrid":
                    dataset_to_alg_to_additional[dataset][alg].append(
                        float(df_sub.iloc[i]["additional_at_least_" + alg])
                    )
                else:
                    dataset_to_alg_to_additional[dataset][alg].append(0.0)
            dataset_to_alg_to_overheads[dataset][alg] = np.array(
                dataset_to_alg_to_overheads[dataset][alg]
            )
            dataset_to_alg_to_additional[dataset][alg] = np.array(
                dataset_to_alg_to_additional[dataset][alg]
            )

    N_scale = dataset_to_alg_to_overheads["scale"]["seq"].shape[0]
    N_stats_ceb = dataset_to_alg_to_overheads["stats_ceb"]["seq"].shape[0]

    fig, (ax1, ax2) = plt.subplots(
        1,
        2,
        figsize=(10, 3.5),
        gridspec_kw={"width_ratios": [N_scale, N_stats_ceb], "wspace": 0.1},
    )
    plt.subplots_adjust(left=0.07, right=0.98, bottom=0.15, top=0.9)

    spacing = 1.5

    for ax, dataset, N in [(ax1, "scale", N_scale), (ax2, "stats_ceb", N_stats_ceb)]:
        x = np.arange(N) * spacing
        bar_width = 0.2
        offsets = [-bar_width, 0, bar_width, bar_width * 2]

        for i, alg in enumerate(["seq", "postfilter", "tree_batch_cext", "hybrid"]):
            ax.bar(
                x + offsets[i],
                dataset_to_alg_to_overheads[dataset][alg],
                bar_width,
                color=colors[alg],
                edgecolor="black",
                linewidth=0.5,
                label=f"{alg_to_formal_name[alg]}: runtime of avail. Qs",
            )

            ax.bar(
                x + offsets[i],
                dataset_to_alg_to_additional[dataset][alg],
                bar_width,
                bottom=dataset_to_alg_to_overheads[dataset][alg],
                color=colors[alg],
                edgecolor="black",
                linewidth=0.5,
                hatch="//",
                label=f"{alg_to_formal_name[alg]}: min. runtime of unavail. Qs",
            )

        if dataset == "scale":
            ax.legend(ncol=2, fontsize=8)
            ax.set_ylabel("LOG Runtime", fontsize=10)
        ax.set_yscale("log")
        ax.set_ylim(1e-1, 1e3)
        # if dataset == "stats_ceb":

        ax.set_title(dataset_to_formal_name[dataset], fontsize=12)

        ax.set_xlim(-1, (N - 1) * spacing + 1)
        ax.tick_params(axis="y", labelsize=10)
        tick_idx = np.arange(0, N)
        ax.set_xticks(x[tick_idx])
        ax.set_xticklabels(dataset_to_pattern_id[dataset], fontsize=10)

        ax.grid(True, axis="y", linestyle=":", linewidth=0.6, alpha=0.6)

    fig.text(0.3, 0.02, "Join Pattern Index (where a baseline is chosen)", fontsize=12)
    # plt.tight_layout(pad=0.4)
    plt.savefig(f"../figures/hybrid_bar_per_join_pattern.png", dpi=300)


if __name__ == "__main__":
    # plot_ordered_runtime()
    ### plot_cumulative_runtime()
    # plot_per_workload_barplot()
    # plot_model_importance("../models/20251230_075047_importance.txt")
    plot_hybrid_per_pattern_barplot()
