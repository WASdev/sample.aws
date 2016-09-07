package test;

import java.io.IOException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@WebServlet(value="/")
public class TestServlet extends HttpServlet { 
  
  public void doGet (HttpServletRequest req, HttpServletResponse resp) throws IOException { 
    resp.getWriter().println ("Hello world from edits made in Mark's repo, updated"); 
  }

}
