import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import React, { useState } from "react";
import "./index.css";

const Tree = (props) => {
  return (
    <div className="d-tree">
      <ul className="d-flex d-tree-container flex-column">
        {props.data.map((tree) => (
          <TreeNode node={tree} searchState={props.searchState} setSearchState={props.setSearchState} key={tree.key} />
        ))}
      </ul>
    </div>
  );
};

const TreeNode = (props) => {
  const [childVisible, setChildVisiblity] = useState(false);

  const hasChild = props.node.children ? true : false;

  const handleClick = (node, event) => {
    document.getElementById('auto_complete').value = ''
    
    props.setSearchState({
        'query': {
          'sort': 'object_name',
          'sort_dir': 'asc',
          't': 'path',
          'q': '',
          'per_page': 50,
          'page': 1,
          'filters': {},
          'bucket': node.bucket,
          'path': node.path.replace(/^\/+|\/+$/g, '')
        }});
  }

  return (
    <li className="d-tree-node border-0">
      <div className="d-flex" onClick={(e) => setChildVisiblity((v) => !v)}>
        {hasChild && (
          <div
            className={`d-inline d-tree-toggler ${
              childVisible ? "active" : ""
            }`}
          >
            <FontAwesomeIcon icon="caret-right" />
          </div>
        )}

        <div className="col d-tree-head">
          <i className={`mr-1 ${props.node.icon}`}> </i>
          <a href="#" onClick={(event) => handleClick(props.node, event)}>{props.node.label}</a>
        </div>
      </div>

      {hasChild && childVisible && (
        <div className="d-tree-content">
          <ul className="d-flex d-tree-container flex-column">
            <Tree data={props.node.children} searchState={props.searchState} setSearchState={props.setSearchState} key="{props.node.key}" />
          </ul>
        </div>
      )}
    </li>
  );
};

export default Tree;
