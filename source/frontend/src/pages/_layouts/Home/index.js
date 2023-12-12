import React, { useEffect } from "react";
//import { Navbar } from "./Navbar";
//import { useSelector } from "react-redux";

import { withRouter } from "react-router-dom";
import AppConfig from "App.config";

import Filebrowser from "components/Filebrowser"

const Home = ({ children }) => {
    return (
        <>
            <Filebrowser />
        </>
    );
};

export default withRouter(Home);
