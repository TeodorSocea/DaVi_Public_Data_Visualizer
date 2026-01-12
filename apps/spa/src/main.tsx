import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter, Routes, Route } from "react-router-dom";
import SearchPage from "./pages/SearchPage";
import CategoryPage from "./pages/CategoryPage";
import EntityPage from "./pages/EntityPage";
import "./extensions";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<SearchPage />} />
        <Route path="/category/:id" element={<CategoryPage />} />
        <Route path="/entity/:id" element={<EntityPage />} />
      </Routes>
    </BrowserRouter>
  </React.StrictMode>
);
