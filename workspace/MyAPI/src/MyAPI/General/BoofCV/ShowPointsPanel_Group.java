/*
 * Copyright (c) 2011-2012, Peter Abeles. All Rights Reserved.
 *
 * This file is part of BoofCV (http://boofcv.org).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package MyAPI.General.BoofCV;

import MyAPI.General.General_BoofCV;
import MyCustomedHaoop.ValueClass.SURFpoint;

import java.awt.*;
import java.awt.event.MouseListener;
import java.util.LinkedList;
import java.util.List;


/**
 * Shows which two features are associated with each other.  An individual feature
 * can be shown alone by clicking on it.
 *
 */
@SuppressWarnings("serial")
public class ShowPointsPanel_Group extends CompareTwoImagePanel_DIY implements MouseListener {
	
	protected LinkedList<LinkedList<SURFpoint>> selPointInd_l;
	protected LinkedList<LinkedList<SURFpoint>> selPointInd_r;
	protected LinkedList<Color> setColors;
	
	public ShowPointsPanel_Group(int borderSize, int pointEnlargeFactor) {
		super(borderSize, true, pointEnlargeFactor);		
	}

	public synchronized void setPoints(LinkedList<LinkedList<SURFpoint>> selPointInd_l , LinkedList<LinkedList<SURFpoint>> selPointInd_r, LinkedList<Color> setColors) {
		this.selPointInd_l=selPointInd_l;
		this.selPointInd_r=selPointInd_r;
		this.setColors=setColors;
	}

	@Override
	protected void drawFeatures(Graphics2D g2 ,
							 double scaleLeft, int leftX, int leftY,
							 double scaleRight, int rightX, int rightY) {
		//show left
		if (selPointInd_l!=null) {
			for (int i = 0; i < selPointInd_l.size(); i++) {
				List<SURFpoint> oneList= selPointInd_l.get(i);
				Color oneColor=setColors.get(i);
				for (SURFpoint one : oneList) {
					General_BoofCV.drawInterestPoint(g2, one, scaleLeft, 0, 0, oneColor, pointEnlargeFactor, true);
				}
			}
		}
		//show right
		if (selPointInd_r!=null) {
			for (int i = 0; i < selPointInd_r.size(); i++) {
				List<SURFpoint> oneList= selPointInd_r.get(i);
				Color oneColor=setColors.get(i);
				for (SURFpoint one : oneList) {
					General_BoofCV.drawInterestPoint(g2, one, scaleRight, rightX, 0, oneColor, pointEnlargeFactor, true);
				}
			}
		}
	}

	@Override
	protected boolean isValidPoint(int index) {
		return true;
	}
}
