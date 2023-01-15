const sidebars = {
  sidebar: [
    {
      type: "category",
      label: "ZIO DynamoDB",
      collapsed: false,
      link: { type: "doc", id: "index" },
      items: [ 
        "getting-started",
        "codec-customization",
        "transactions",
      ]
    }
  ]
};

module.exports = sidebars;