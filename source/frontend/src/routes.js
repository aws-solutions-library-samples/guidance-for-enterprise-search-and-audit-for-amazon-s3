import React from "react";

const TreeList = React.lazy(() => import("pages/TreeList"));

const routes = [
  
];

export default routes.filter((route) => route.enabled);
