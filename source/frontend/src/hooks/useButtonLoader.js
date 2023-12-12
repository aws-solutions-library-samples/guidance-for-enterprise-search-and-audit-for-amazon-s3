import { useRef, useState, useEffect } from "react";

const useButtonLoader = (defaultText = "Load", loadingText = "Loading...") => {
    const [isLoading, setLoading] = useState(false);
    const element = useRef(null);

    useEffect(() => {
        if (isLoading) {
            element.current.disabled = true;
            element.current.innerHTML =
                '<i class="fas fa-spinner fa-spin"></i> ' + escapeHTML(loadingText);
        } else {
            element.current.disabled = false;
            element.current.innerHTML = escapeHTML(defaultText);
        }
    }, [isLoading]);

    function escapeHTML (unsafe_str) {
        return unsafe_str
          .replace(/&/g, '&amp;')
          .replace(/</g, '&lt;')
          .replace(/>/g, '&gt;')
          .replace(/\"/g, '&quot;')
          .replace(/\'/g, '&#39;')
          .replace(/\//g, '&#x2F;')
    }

    return [element, setLoading];
};

export default useButtonLoader;
