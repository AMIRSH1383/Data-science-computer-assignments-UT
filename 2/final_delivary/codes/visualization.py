from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.image as mpimg
import os

sns.set_style("white")

client = MongoClient("mongodb://localhost:27017")
db = client["transaction_analysis"]


def save_plot(fig, filename):
    output_dir = "plots"
    os.makedirs(output_dir, exist_ok=True)
    filepath = os.path.join(output_dir, filename)
    fig.savefig(filepath, bbox_inches="tight")
    plt.close(fig)
    return filepath


txn_summary = pd.DataFrame(list(db["transaction_summary"].find({}, {"merchant_category": 1, "payment_method": 1})))

payment_pref = txn_summary.groupby(["merchant_category", "payment_method"]).size().unstack(fill_value=0)
payment_pref_ratio = payment_pref.div(payment_pref.sum(axis=1), axis=0)

merchant_categories = payment_pref_ratio.index.tolist()
payment_methods = ["online", "pos", "mobile", "nfc"]

labels = []
values = []
colors = []

for cat in merchant_categories:
    row = payment_pref_ratio.loc[cat]
    top_method = row.idxmax()
    for method in payment_methods:
        labels.append(f"{cat} - {method}")
        values.append(row[method])
        if method == top_method:
            colors.append((0.0, 0.0, 0.5, 1.0)) 
        else:
            colors.append((0.5, 0.5, 0.5, 0.3)) 

fig9, ax9 = plt.subplots(figsize=(12, 8))
spacing = 1
y_positions = []
ytick_labels = []

pos = 0
for i, cat in enumerate(merchant_categories):
    for method in payment_methods:
        y_positions.append(pos)
        ytick_labels.append(f"{cat} - {method}")
        pos += 1
    pos += spacing 

fig9, ax9 = plt.subplots(figsize=(12, 8))
ax9.barh(y_positions, values, color=colors)
ax9.set_yticks(y_positions)
ax9.set_yticklabels(ytick_labels)

ax9.set_xlabel("Payment Preference Ratio")
ax9.set_title("Preferred Payment Method by Merchant Category")
ax9.invert_yaxis() 
ax9.grid(False)

path9 = save_plot(fig9, "9_payment_preference.png")







#Transactions Volume  
daily_summary = pd.DataFrame(list(db["daily_summary"].find()))
daily_summary.sort_values("_id", inplace=True)
daily_summary["_id"] = pd.to_datetime(daily_summary["_id"], format="%Y-%m-%d")

fig1, ax1 = plt.subplots(figsize=(12, 6))
ax1.plot(daily_summary["_id"], daily_summary["total_transactions"], label="Total", marker='o', color='gray', alpha=0.5)
ax1.plot(daily_summary["_id"], daily_summary["approved_transactions"], label="Approved", marker='o', color='navy', alpha=0.8)
ax1.plot(daily_summary["_id"], daily_summary["declined_transactions"], label="Declined", marker='o', color='red', alpha=0.7)
ax1.set_xlabel("Date")
ax1.set_ylabel("Transactions")
ax1.set_title("Daily Transaction Volume")
ax1.set_xticklabels(daily_summary["_id"].dt.strftime('%d %b'))
ax1.legend()
ax1.grid(False)
path1 = save_plot(fig1, "1_daily_volume.png")

#Top 10 Merchants by Transaction Count 
merchant_summary = pd.DataFrame(list(db["merchant_summary"].find()))
top_merchants = merchant_summary.nlargest(10, "total_trxs")

colors = ["navy" if i < 3 else "gray" for i in range(len(top_merchants))]
alphas = [1.0, 0.8, 0.6] + [0.3] * 7

fig2, ax2 = plt.subplots(figsize=(8, 4.8))
top_sorted = top_merchants.sort_values("total_trxs", ascending=False)
bars = ax2.barh(top_sorted["_id"], top_sorted["total_trxs"], color=[(c, a) for c, a in zip(colors, alphas)])
ax2.set_xlabel("Number of Transactions")
ax2.set_ylabel("Merchant ID")
ax2.set_title("Top 10 Merchants by Number of Transactions")
ax2.invert_yaxis()
ax2.grid(False)
path2 = save_plot(fig2, "2_top_merchants.png")

#User Activity 
customer_summary = pd.DataFrame(list(db["customer_summary"].find()))
top_customers = customer_summary.nlargest(10, "total_trxs")

colors_c = ["navy" if i < 3 else "gray" for i in range(len(top_customers))]
alphas_c = [1.0, 0.8, 0.6] + [0.3] * 7

fig3, ax3 = plt.subplots(figsize=(10, 6))
cust_sorted = top_customers.sort_values("total_trxs", ascending=False)
bars = ax3.barh(cust_sorted["_id"], cust_sorted["total_trxs"], color=[(c, a) for c, a in zip(colors_c, alphas_c)])
ax3.set_xlabel("Transactions")
ax3.set_ylabel("Customer ID")
ax3.set_title("Top 10 Most Active Customers")
ax3.invert_yaxis()
ax3.grid(False)
path3 = save_plot(fig3, "3_top_customers.png")

#Average Risk Level per Customer Type
cust_cat_summary = pd.DataFrame(list(db["customer_category_summary"].find()))
top_risk = cust_cat_summary["avg_risk_level"].idxmax()

fig4, ax4 = plt.subplots(figsize=(8, 5))
colors_r = ["darkred" if i == top_risk else "gray" for i in range(len(cust_cat_summary))]
alphas_r = [1.0 if i == top_risk else 0.3 for i in range(len(cust_cat_summary))]
bars = ax4.barh(cust_cat_summary["_id"], cust_cat_summary["avg_risk_level"], color=[(c, a) for c, a in zip(colors_r, alphas_r)])
ax4.set_xlabel("Avg Risk Level")
ax4.set_ylabel("Customer Type")
ax4.set_title("Risk Level by Customer Type")
ax4.invert_yaxis()
ax4.grid(False)
path4 = save_plot(fig4, "4_risk_level.png")

#Commission to Transaction Ratio by Merchant Category
merchant_cat_summary = pd.DataFrame(list(db["merchant_category_summary"].find()))
top2 = merchant_cat_summary.nlargest(2, "commission_to_transaction_ratio")["_id"].tolist()
merchant_cat_summary["color"] = merchant_cat_summary["_id"].apply(lambda x: "navy" if x in top2 else "gray")
merchant_cat_summary["alpha"] = merchant_cat_summary["_id"].apply(lambda x: 1.0 if x == top2[0] else (0.8 if x == top2[1] else 0.3))

fig5, ax5 = plt.subplots(figsize=(10, 6))
merchant_cat_sorted = merchant_cat_summary.sort_values("commission_to_transaction_ratio", ascending=False)
bars = ax5.barh(merchant_cat_sorted["_id"], merchant_cat_sorted["commission_to_transaction_ratio"],
                color=[(c, a) for c, a in zip(merchant_cat_sorted["color"], merchant_cat_sorted["alpha"])])
ax5.set_xlabel("Commission to Transaction Ratio")
ax5.set_ylabel("Merchant Category")
ax5.set_title("Commission Ratio by Merchant Category")
ax5.invert_yaxis()
ax5.grid(False)
path5 = save_plot(fig5, "5_commission_ratio.png")

#6. Most Expensive Merchant Categories ===
merchant_cat_summary["avg_transaction_value"] = merchant_cat_summary["total_transaction_value"] / merchant_cat_summary["total_successful_txns"]
top2_exp = merchant_cat_summary.nlargest(2, "avg_transaction_value")["_id"].tolist()
merchant_cat_summary["color_exp"] = merchant_cat_summary["_id"].apply(lambda x: "navy" if x in top2_exp else "gray")
merchant_cat_summary["alpha_exp"] = merchant_cat_summary["_id"].apply(lambda x: 1.0 if x == top2_exp[0] else (0.8 if x == top2_exp[1] else 0.3))

fig6, ax6 = plt.subplots(figsize=(10, 6))
merchant_cat_exp_sorted = merchant_cat_summary.sort_values("avg_transaction_value", ascending=False)
bars = ax6.barh(merchant_cat_exp_sorted["_id"], merchant_cat_exp_sorted["avg_transaction_value"],
                color=[(c, a) for c, a in zip(merchant_cat_exp_sorted["color_exp"], merchant_cat_exp_sorted["alpha_exp"])])
ax6.set_xlabel("Avg Transaction Value")
ax6.set_ylabel("Merchant Category")
ax6.set_title("Most Expensive Merchant Categories")
ax6.invert_yaxis()
ax6.grid(False)
path6 = save_plot(fig6, "6_expensive_categories.png")

#Most Declined Customer Categories ===
customer_summary["declined_ratio"] = customer_summary["declined_trxs"] / customer_summary["total_trxs"]
top_declined = customer_summary.nlargest(10, "declined_ratio")
top3_declined = top_declined["_id"].tolist()[:3]
top_declined["color"] = top_declined["_id"].apply(lambda x: "darkred" if x in top3_declined else "gray")
top_declined["alpha"] = top_declined["_id"].apply(lambda x: 1.0 if x == top3_declined[0] else (0.8 if x == top3_declined[1] else (0.6 if x == top3_declined[2] else 0.3)))

fig7, ax7 = plt.subplots(figsize=(10, 6))
decl_sorted = top_declined.sort_values("declined_ratio", ascending=False)
bars = ax7.barh(decl_sorted["_id"], decl_sorted["declined_ratio"],
                color=[(c, a) for c, a in zip(decl_sorted["color"], decl_sorted["alpha"])])
ax7.set_xlabel("Declined / Total Transactions")
ax7.set_ylabel("Customer ID")
ax7.set_title("Top 10 Most Declined Customers")
ax7.invert_yaxis()
ax7.grid(False)
path7 = save_plot(fig7, "7_most_declined.png")




#Most valued merchant category ===
merchant_cat_summary = pd.DataFrame(list(db["merchant_category_summary"].find()))
top2 = merchant_cat_summary.nlargest(2, "total_transaction_value")["_id"].tolist()
merchant_cat_summary["color"] = merchant_cat_summary["_id"].apply(lambda x: "navy" if x in top2 else "gray")
merchant_cat_summary["alpha"] = merchant_cat_summary["_id"].apply(lambda x: 1.0 if x == top2[0] else (0.8 if x == top2[1] else 0.3))

fig8, ax8 = plt.subplots(figsize=(10, 6))
merchant_cat_sorted = merchant_cat_summary.sort_values("total_transaction_value", ascending=False)
bars = ax8.barh(merchant_cat_sorted["_id"], merchant_cat_sorted["total_transaction_value"],
                color=[(c, a) for c, a in zip(merchant_cat_sorted["color"], merchant_cat_sorted["alpha"])])
ax8.set_xlabel("total transaction value")
ax8.set_ylabel("Merchant Category")
ax8.set_title("total transaction value by Merchant Category")
ax8.invert_yaxis()
ax8.grid(False)
path8 = save_plot(fig8, "8_commission_ratio.png")

#Plot all together as a dashboard
fig, axs = plt.subplots(3, 3, figsize=(24, 24))

axs = axs.flatten()



for i, path in enumerate([path1, path2, path3, path4, path5, path6, path7, path8, path9]):
    img = mpimg.imread(path)
    axs[i].imshow(img)
    axs[i].axis('off')
    for spine in axs[i].spines.values():
        spine.set_visible(False)

plt.tight_layout()
plt.show()
