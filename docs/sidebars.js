const sidebars = {
  sidebar: [
    {
      type: "category",
      label: "ZIO DynamoDB 3.x",
      collapsed: false,
      link: { type: "doc", id: "index" },
      items: [
        {
          type: "category",
          label: "CRUD Operations",
          link: { type: "doc", id: "reference/crud/index" },
          collapsed: false,
          items: [
            "reference/crud/low-level",
            "reference/crud/high-level",
            "reference/crud/batch",
            "reference/crud/transactions"
          ]
        },
        "reference/codec",
        "reference/interceptor",
        "reference/retries",
        "reference/limitations",
        "reference/examples"
      ]
    }
  ]
};

module.exports = sidebars;
