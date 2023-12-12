import React from "react";
import Tree from "components/Tree";

export const Navbar = (props) => {
    const url = process.env.REACT_APP_API_GATEWAY_URL + 'gettree'
    const [treeData, setTreeData] = React.useState([])

    React.useEffect(() => {
        async function fetchData() {
            const res = await fetch(url);
            const data = await res.json();
            //console.log(data)
            setTreeData(data);
        }
        
        fetchData().catch(console.error);
    }, []);

  return (
    <div className="col-2 text-left" id="sidebar">
        <div className="row mt-3 justify-content-left">
            <div className="text-left text-dark">
            <Tree data={treeData} searchState={props.searchState} setSearchState={props.setSearchState}  />
            </div>
        </div>
    </div>
  );
};

export default Navbar;